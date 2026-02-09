"""MongoDB -> AuraDB polyglot persistence utility functions"""

# Imports
from collections import defaultdict
from loguru import logger
from .embedders import GemmaEmbedder


collection_label_map = {
    "books": "Book",
    "book_versions": "BookVersion",
    "book_series": "BookSeries",
    "genres": "Genre",
    "awards": "Award",
    "creators": "Creator",
    "creator_roles": "CreatorRole",
    "publishers": "Publisher",
    "formats": "Format",
    "languages": "Language",
    "users": "User",
    "clubs": "Club",
    "user_badges": "UserBadge",
    "club_badges": "ClubBadge",
    "countries": "Country",
}


def process_books(books):
    """Convert book award data to str and embed descriptions."""

    # Generate list of dicts with book award data
    book_awards = []

    for book in books:
        book_id = book.get("_id")
        awards = book.get("awards", [])

        for award in awards:
            book_awards.append({
                "book_id": book_id,
                "award_id": award.get("_id"),
                "award_name": award.get("name", ""),
                "award_category": award.get("category", ""),
                "award_year": award.get("year"),
                "award_status": award.get("status", "")
            })

    # Convert book awards list to dict by concatenating list members
    ba_map = {}
    u_ids = set(i["book_id"] for i in book_awards)

    for _id in u_ids:
        awards_list = []
        award_docs = [i for i in book_awards if i["book_id"] == _id]

        for ad in award_docs:
            award = ad["award_name"]
            if ad["award_category"] != "":
                award = f"{ad["award_name"]} for {ad["award_category"]}"
            awards_list.append(f"{award}, {ad["award_year"]}, {ad["award_status"]}")

        str_awards = "; ".join(str(i) for i in awards_list)
        ba_map[_id] = str_awards

    # Replace awards entries in books with new awards strings
    for book in books:
        book["awards"] = ba_map.get(book["_id"], None)
        if book["awards"] is None:
            book.pop("awards")

    # Enrich and embed book descriptions
    descriptions = []
    valid_books = []
    for book in books:
        try:
            combo = (
                f"Title: {book["title"]}"
                f"\n\nAuthor: {", ".join(str(k) for k in book["author"])}"
                f"\n\nGenres: {", ".join(str(k) for k in book["genre"])}"
                f"\n\nDescription: {book["description"]}"
                )
            descriptions.append(combo)
            valid_books.append(book)
        except KeyError:
            logger.warning(f"Description not found for {book["title"]}")
            continue

    if descriptions:
        model = GemmaEmbedder.instance()
        embeddings = model.vectorise_many(descriptions)
        for i, book in enumerate(valid_books):
            book["description_embedding"] = embeddings[i]

    return books, book_awards


def proceess_ur(user_reads):
    """
    Aggregate user/version reading entries to obtain:
    - most recent rstatus
    - most recent start date
    - most recent read date
    - read count
    - avg rating
    - avg days to read
    - avg read rate (pages_per_day or hours_per_day)
    """
    agg_ur = []
    version_ids = set(i["version_id"] for i in user_reads)
    user_ids = set(i["user_id"] for i in user_reads)
    priority = {"Read": 3, "Reading": 2, "Paused": 1, "To Read": 0}

    for u_id in user_ids:
        for v_id in version_ids:

            # All entries for this user + version
            entries = [
                e for e in user_reads
                if e["user_id"] == u_id and e["version_id"] == v_id
            ]
            if not entries:
                continue

            # Flatten reading logs
            logs = []
            for e in entries:
                if "reading_log" in e and e["reading_log"]:
                    logs.extend(e["reading_log"])

            if logs:
                # Most recent rstatus
                most_recent_event = max(logs, key=lambda x:
                                        (x["timestamp"], priority.get(x["rstatus"], -1)))
                most_recent_rstatus = most_recent_event["rstatus"]

                # Most recent start ("Reading")
                reading_events = [l for l in logs if l["rstatus"] == "Reading"]
                most_recent_start = (
                    max(reading_events, key=lambda x: x["timestamp"])["timestamp"]
                    if reading_events else None
                )

                # Most recent read ("Read")
                read_events = [l for l in logs if l["rstatus"] == "Read"]
                most_recent_read = (
                    max(read_events, key=lambda x: x["timestamp"])["timestamp"]
                    if read_events else None
                )

                read_count = len(read_events)

            else:
                # no logs likely means "To Read"
                most_recent_rstatus = "To Read"
                most_recent_start = None
                most_recent_read = None
                read_count = 0

            # Most recent review
            try:
                most_recent_review = [i["notes"] for i in entries][0]
            except KeyError:
                most_recent_review = None

            # Averages
            ratings = [e.get("rating") for e in entries if e.get("rating") is not None]
            avg_rating = sum(ratings) / len(ratings) if ratings else None

            d2r = [e.get("days_to_read") for e in entries if e.get("days_to_read")]
            avg_days_to_read = sum(d2r) / len(d2r) if d2r else None

            rates = []
            for e in entries:
                if "pages_per_day" in e and e["pages_per_day"]:
                    rates.append(e["pages_per_day"])
                elif "hours_per_day" in e and e["hours_per_day"]:
                    rates.append(e["hours_per_day"])
            avg_read_rate = sum(rates) / len(rates) if rates else None

            agg_ur.append({
                "user_id": u_id,
                "version_id": v_id,
                "most_recent_rstatus": most_recent_rstatus,
                "most_recent_start": most_recent_start,
                "most_recent_read": most_recent_read,
                "most_recent_review": most_recent_review,
                "read_count": read_count,
                "avg_rating": avg_rating,
                "avg_days_to_read": avg_days_to_read,
                "avg_read_rate": avg_read_rate,
            })

    return agg_ur


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
        coll_name = doc.get("original_collection")
        label = collection_label_map.get(coll_name)
        if label:
            label_groups[label].append(str(doc["_id"]))

    # Execute batch deletions in AuraDB
    with driver.session() as session:
        for label, ids in label_groups.items():
            query = f"MATCH (n:{label}) WHERE n._id IN $ids DETACH DELETE n"
            session.run(query, ids=ids)
            logger.success(f"Deleted {len(ids)} '{label}' nodes from AuraDB.")


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
    for label, props in label_props.items():
        logger.info(f"Cleaning up {label} nodes")

        total_nodes_touched = 0
        removal_counts = {prop: 0 for prop in props}

        for prop in props:
            while True:
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
    agg_ur = proceess_ur(user_reads)
    if not agg_ur:
        return

    rel_map = {
        "DNF": "DID_NOT_FINISH", "Read": "HAS_READ", "Paused": "HAS_PAUSED",
        "Reading": "IS_READING", "To Read": "WANTS_TO_READ"
    }

    rows = []
    for doc in agg_ur:
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
    SET rel.mostRecentStart = row.most_recent_start,
        rel.mostRecentRead  = row.most_recent_read,
        rel.mostRecentReview = row.most_recent_review,
        rel.readCount       = row.read_count,
        rel.avgRating       = row.avg_rating,
        rel.avgDaysToRead   = row.avg_days_to_read,
        rel.avgReadRate     = row.avg_read_rate
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
        SET rel.period = row.period, rel.startDate = row.startdate,
            rel.endDate = row.enddate, rel.selectionMethod = row.selection_method
        """
        tx.run(merge_query, rows=merge_rows)

    logger.info(f"Created or updated {len(prune_rows)} Club-Book relationships.")
