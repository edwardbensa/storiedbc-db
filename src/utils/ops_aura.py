"""MongoDB -> AuraDB polyglot persistence utility functions"""

# Imports
from collections import defaultdict
from loguru import logger
from .transform_for_aura import collection_label_map


def sync_deletions(driver, db, since):
    """
    Fetch deletions from MongoDB and remove corresponding nodes in AuraDB.
    """
    # Fetch deletions since last sync
    query = {"deleted_at": {"$gt": since}}
    deletions = list(db["deletions"].find(query))

    if not deletions:
        logger.info("No new deletions to sync.")
        return

    # Group IDs by their Neo4j label using the provided map
    label_groups = defaultdict(list)
    for doc in deletions:
        collection_name = doc.get("original_collection")
        original_id = doc.get("original_id")

        if not collection_name or not original_id:
            logger.warning(f"Skipping malformed deletion record: {doc}.")
            continue

        label = collection_label_map.get(collection_name)
        if not label:
            logger.warning(f"No Neo4j label mapped for collection '{collection_name}'.")
            continue

        label_groups[label].append(str(doc["_id"]))

    # Execute batch deletions in AuraDB
    try:
        with driver.session() as session:
            for label, ids in label_groups.items():
                query = f"MATCH (n:{label}) WHERE n._id IN $ids DETACH DELETE n"

                logger.debug(f"Deleting {len(ids)} nodes with label '{label}'")

                result = session.run(query, ids=ids)
                summary = result.consume()

                logger.success(
                    f"Deleted {summary.counters.nodes_deleted} '{label}' nodes "
                    f"({summary.counters.relationships_deleted} relationships)"
                )
    except Exception as e:
        logger.error(f"Failed to sync deletions: {e}")
        raise


def deduplicate_nodes(driver, label, identifier="_id"):
    """
    Removes duplicate nodes in Neo4j Aura based on the specified identifier property.
    Keeps one node per identifier and deletes all others (including relationships).

    Parameters:
        driver: Neo4j driver instance
        label (str): Node label to check for duplicates (e.g., "Person")

    Returns:
        int: Number of duplicate nodes deleted
    """

    delete_query = f"""
    MATCH (n:{label})
    WHERE n.{identifier} IS NOT NULL
    WITH n.{identifier} AS id, collect(n) AS nodes
    WHERE size(nodes) > 1
    CALL (nodes) {{
        WITH nodes
        WITH nodes[0] AS keep, nodes[1..] AS duplicates
        UNWIND duplicates AS d
        DETACH DELETE d
        RETURN count(d) AS deleted_count
    }}
    RETURN sum(deleted_count) AS total_deleted
    """

    with driver.session() as session:
        result = session.run(delete_query)
        deleted_count = result.single()["total_deleted"] or 0

    logger.info(f"Number of duplicate nodes deleted for {label} label: {deleted_count}")

    return deleted_count


def upsert_nodes(tx, label, rows, id_field="_id"):
    """Generic AuraDB upsert function"""
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{_id: row.{id_field}}})
    SET n += row
    """
    tx.run(query, rows=rows)
    logger.success(f"Upserted {len(rows)} '{label}' nodes into Neo4j database.")


def clear_all_nodes(driver):
    """Clear all nodes in graph."""
    query = "MATCH (n) DETACH DELETE n"
    with driver.session() as session:
        session.run(query)
    logger.info("All nodes and relationships cleared.")


def cleanup_nodes(driver, label_props: dict, batch_size: int = 5000):
    """
    Remove specified properties from nodes of given labels, safely and in batches.
    """
    max_iterations: int = 1000

    for label, props in label_props.items():
        logger.info(f"Cleaning up {label} nodes")

        total_nodes_touched = 0
        removal_counts = {prop: 0 for prop in props}

        for prop in props:
            iterations = 0
            while iterations < max_iterations:
                iterations += 1

                query = f"""
                MATCH (n:{label})
                WHERE n.{prop} IS NOT NULL
                WITH n LIMIT $batch_size
                REMOVE n.{prop}
                RETURN count(n) AS removed
                """

                with driver.session() as session:
                    result = session.run(query, batch_size=batch_size)
                    removed = result.single()["removed"]

                if removed == 0:
                    break

                removal_counts[prop] += removed
                total_nodes_touched += removed

        # Build readable summary
        removal_summary = ", ".join(
            f"{prop}={count}" for prop, count in removal_counts.items()
        )

        logger.success(
            f"Cleaned up {label}. n_nodes={total_nodes_touched}. Removals: {removal_summary}"
        )


def ensure_constraints(driver, constraints_map):
    """
    Ensure unique constraints exist for all primary identifiers.
    """
    with driver.session() as session:
        for label, prop in constraints_map.items():
            query = (
                f"CREATE CONSTRAINT {label.lower()}_{prop}_unique "
                f"IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE"
                )
            session.run(query)
            logger.info(f"Verified constraint for {label}({prop})")


def create_relationships(tx, rel_map, rel: str, source_docs):
    """
    Create relationship between two labels
    while pruning old relationships for the updated batch.
    """
    # Extract node labels, fields, updated source _ids
    source_label = rel_map["labels"][0]
    target_label = rel_map["labels"][1]
    source_prop = rel_map["props"][0]
    target_prop = rel_map["props"][1]
    updated_ids = [i["_id"] for i in source_docs]

    if not updated_ids:
        return

    # Delete only the specific relationship type for nodes being updated
    query = f"""
    MATCH (source:{source_label})
    WHERE source._id IN $ids
    OPTIONAL MATCH (source)-[old_rel:{rel}]->()
    DELETE old_rel
    WITH source
    WHERE source.{source_prop} IS NOT NULL
    UNWIND source.{source_prop} AS value
    MATCH (target:{target_label} {{{target_prop}: value}})
    MERGE (source)-[:{rel}]->(target)
    RETURN count(*) AS relationships_created
    """

    result = tx.run(query, ids=updated_ids)
    count = result.single()["relationships_created"]
    logger.info(f"Created {count} relationships of type {rel}")


def badges_relationships(tx, docs: list, label="User"):
    """
    Create relationships between users/clubs and the badges they earn.
    Pruning old relationships to allow for badge removals/corrections.
    """
    if not docs:
        return

    if label not in ["User", "Club"]:
        raise ValueError("Label must be 'User' or 'Club'.")

    source_label = label
    target_label = "UserBadge" if label == "User" else "ClubBadge"
    updated_ids = [doc["_id"] for doc in docs]

    # Prune all HAS_BADGE relationships for updated nodes
    prune_query = f"""
    MATCH (source:{source_label})
    WHERE source._id IN $ids
    OPTIONAL MATCH (source)-[r:HAS_BADGE]->()
    DELETE r
    """
    tx.run(prune_query, ids=updated_ids)

    # Merge the current badge list
    rows = []
    for doc in docs:
        label_id = doc.get("_id")
        badges = doc.get("badges") or []
        timestamps = doc.get("badge_timestamps") or []
        for badge, earned_on in zip(badges, timestamps):
            rows.append({"label_id": label_id, "badge": badge, "earned_on": earned_on})

    if rows:
        merge_query = f"""
        UNWIND $rows AS row
        MATCH (a:{source_label} {{_id: row.label_id}})
        MATCH (b:{target_label} {{name: row.badge}})
        MERGE (a)-[rel:HAS_BADGE]->(b)
        SET rel.earnedOn = row.earned_on
        """
        tx.run(merge_query, rows=rows)

    logger.info(f"Refreshed {len(rows)} {source_label}-Badge relationships.")


def user_reads_relationships(tx, user_reads):
    """
    Create relationships between users and the books they read,
    using aggregated stats with pre-cleanup to handle status transitions
    """

    rel_map = {
        "DNF": "DID_NOT_FINISH", "Read": "HAS_READ", "Paused": "HAS_PAUSED",
        "Reading": "IS_READING", "To Read": "WANTS_TO_READ"
    }

    rows = []
    for doc in user_reads:
        rel_type = rel_map.get(doc.get("most_recent_rstatus"))
        if not rel_type:
            continue

        rows.append({**doc, "rel_type": rel_type})

    # Pre-cleanup existing status relationships for these specific user-version pairs
    cleanup_query = """
    UNWIND $rows AS row
    MATCH (u:User {_id: row.user_id})-[r:DID_NOT_FINISH|HAS_READ|HAS_PAUSED|IS_READING|WANTS_TO_READ]->(b:BookVersion {_id: row.version_id})
    DELETE r
    """
    tx.run(cleanup_query, rows=rows)

    # Merge new status relationship
    merge_query = """
    UNWIND $rows AS row
    MATCH (u:User {_id: row.user_id})
    MATCH (b:BookVersion {_id: row.version_id})
    CALL apoc.merge.relationship(u, row.rel_type, {}, {}, b) YIELD rel
    SET rel.most_recent_start   = row.most_recent_start,
        rel.most_recent_read    = row.most_recent_read,
        rel.most_recent_review  = row.most_recent_review,
        rel.read_count          = row.read_count,
        rel.avg_rating          = row.avg_rating,
        rel.avg_days_to_read    = row.avg_days_to_read,
        rel.avg_read_rate       = row.avg_read_rate
    """
    tx.run(merge_query, rows=rows)
    logger.info(f"Updated reading status for {len(rows)} User-BookVersion pairs.")


def book_awards_relationships(tx, updated_books, award_rows):
    """
    Create HAS_AWARD relationships between Book and Award labels.
    Prune and merge existing relationships to facilitate updates.
    """
    if not updated_books:
        return

    # Prune existing awards for the books in this batch
    updated_book_ids = [b["_id"] for b in updated_books]
    prune_query = """
    MATCH (b:Book)
    WHERE b._id IN $ids
    OPTIONAL MATCH (b)-[r:HAS_AWARD]->()
    DELETE r
    """
    tx.run(prune_query, ids=updated_book_ids)

    # Merge new award data
    if award_rows:
        merge_query = """
        UNWIND $rows AS row
        MATCH (b:Book {_id: row.book_id})
        MATCH (a:Award {_id: row.award_id})
        MERGE (b)-[rel:HAS_AWARD]->(a)
        SET rel.status = row.award_status,
            rel.year = row.award_year
            FOREACH (_ IN CASE WHEN row.award_category <> "" THEN [1] ELSE [] END |
                SET rel.category = row.award_category
            )
        """
        tx.run(merge_query, rows=award_rows)

    logger.info(f"Created or updated {len(updated_book_ids)} Book-Award relationships")


def club_book_relationships(tx, club_period_books):
    """
    Create SELECTED_FOR_PERIOD relationships between Club and Book.
    Refresh club selections by pruning specific club/book/period triples.
    """
    if not club_period_books:
        return

    # Prune triples found in the delta
    prune_rows = [{"club_id": r["club_id"], "book_id": r["book_id"], "period": r["period_name"]}
                  for r in club_period_books]
    prune_query = """
    UNWIND $rows AS row
    MATCH (c:Club {_id: row.club_id})-[r:SELECTED_FOR_PERIOD]->(b:Book {_id: row.book_id})
    WHERE r.period = row.period
    DELETE r
    """
    tx.run(prune_query, rows=prune_rows)

    # Merge only currently 'selected' books
    merge_rows = []
    for row in club_period_books:
        if row["selection_status"] == "selected":
            merge_rows.append({
                "club_id": row["club_id"], "book_id": row["book_id"],
                "period": row["period_name"], "startdate": row["period_startdate"],
                "enddate": row["period_enddate"], "selection_method": row["selection_method"]
            })

    if merge_rows:
        merge_query = """
        UNWIND $rows AS row
        MATCH (c:Club {_id: row.club_id})
        MATCH (b:Book {_id: row.book_id})
        MERGE (c)-[rel:SELECTED_FOR_PERIOD]->(b)
        SET rel.period = row.period, rel.startdate = row.startdate,
            rel.enddate = row.enddate, rel.selection_method = row.selection_method
        """
        tx.run(merge_query, rows=merge_rows)

    logger.info(f"Created or updated {len(prune_rows)} Club-Book relationships.")


# Relationship maps

def get_relationships(data):
    """Return relationship mapping."""

    rel_maps = [
        ({"labels": ["Book", "Genre"], "props": ["genre", "name"]},
         "HAS_GENRE", data["books"]),
        ({"labels": ["BookVersion", "Book"], "props": ["book_id", "_id"]},
         "VERSION_OF", data["book_versions"]),
        ({"labels": ["Book", "BookSeries"], "props": ["series_id", "_id"]},
         "ENTRY_IN", data["books"]),
        ({"labels": ["Book", "Creator"], "props": ["author_id", "_id"]},
         "AUTHORED_BY", data["books"]),
        ({"labels": ["BookVersion", "Creator"], "props": ["narrator_id", "_id"]},
         "NARRATED_BY", data["book_versions"]),
        ({"labels": ["BookVersion", "Creator"], "props": ["cover_artist_id", "_id"]},
         "COVER_ART_BY", data["book_versions"]),
        ({"labels": ["BookVersion", "Creator"], "props": ["illustrator_id", "_id"]},
         "ILLUSTRATION_BY", data["book_versions"]),
        ({"labels": ["BookVersion", "Creator"], "props": ["translator_id", "_id"]},
         "TRANSLATED_BY", data["book_versions"]),
        ({"labels": ["BookVersion", "Publisher"], "props": ["publisher_id", "_id"]},
         "PUBLISHED_BY", data["book_versions"]),
        ({"labels": ["BookVersion", "Language"], "props": ["language", "name"]},
         "HAS_LANGUAGE", data["book_versions"]),
        ({"labels": ["BookVersion", "Format"], "props": ["format", "name"]},
         "HAS_FORMAT", data["book_versions"]),
        ({"labels": ["Creator", "CreatorRole"], "props": ["roles", "name"]},
         "HAS_ROLE", data["creators"]),
        ({"labels": ["User", "Club"], "props": ["club_ids", "_id"]},
         "MEMBER_OF", data["users"]),
        ({"labels": ["User", "Country"], "props": ["country", "name"]},
         "LIVES_IN", data["users"]),
        ({"labels": ["User", "Genre"], "props": ["preferred_genres", "name"]},
         "PREFERS_GENRE", data["users"]),
        ({"labels": ["User", "Genre"], "props": ["forbidden_genres", "name"]},
         "AVOIDS_GENRE", data["users"]),
        ({"labels": ["Club", "Genre"], "props": ["preferred_genres", "name"]},
         "PREFERS_GENRE", data["clubs"]),
    ]

    return rel_maps
