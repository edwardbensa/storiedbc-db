"""MongoDB -> AuraDB data transformation utility functions."""

# Imports
from datetime import datetime
from loguru import logger
from .embedders import GemmaEmbedder
from .security import decrypt_field


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
                award = f'{ad["award_name"]} for {ad["award_category"]}'
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
        try:
            embeddings = model.vectorise_many(descriptions)
            for i, book in enumerate(valid_books):
                book["description_embedding"] = embeddings[i]
        except Exception as e: # pylint: disable=W0718
            logger.error(f"Embedding generation failed: {e}. Adding error marker.")
            for i, book in enumerate(valid_books):
                book["failed_embedding"] = True

    return books, book_awards


def process_ur(user_reads):
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


# EXTRACTION FUNCTION

def build_extraction_config(db, lst):
    """
    Build structured set of per-collection extraction instructions from main db.
    """

    # Field maps
    books_map = {
        "author": "author.name",
        "author_id": "author._id",
        "series": "series.name",
        "series_id": "series._id",
    }

    bv_map = {
        "translator": "translator.name",
        "translator_id": "translator._id",
        "illustrator": "illustrator.name",
        "illustrator_id": "illustrator._id",
        "narrator": "narrator.name",
        "narrator_id": "narrator._id",
        "cover_artist": "cover_artist.name",
        "cover_artist_id": "cover_artist._id",
        "contributor": "contributor.name",
        "contributor_id": "contributor._id",
        "publisher_id": "publisher._id",
        "publisher": "publisher.name",
    }

    user_map = {
        "club_ids": "clubs._id",
        "badges": "badges.name",
        "badge_timestamps": "badges.timestamp",
    }

    club_map = {
        "badges": "badges.name",
        "badge_timestamps": "badges.timestamp",
    }

    excluded_user_fields = [
        "firstname",
        "lastname",
        "email_address",
        "password",
        "dob",
        "gender",
        "city",
        "state",
        "is_admin",
        "last_active_date",
    ]

    excluded_club_fields = [
        "member_permissions",
        "join_requests",
        "moderators"
    ]

    # Collection configs
    tasks = {
        "books": {
            "collection": db["books"],
            "field_map": books_map,
            "since": lst,
        },
        "book_versions": {
            "collection": db["book_versions"],
            "field_map": bv_map,
            "since": lst,
        },
        "book_series": {
            "collection": db["book_series"],
            "exclude_fields": ["books"],
            "since": lst,
        },
        "genres": {
            "collection": db["genres"],
            "exclude_fields": ["date_added"],
            "since": lst,
        },
        "awards": {
            "collection": db["awards"],
            "exclude_fields": ["date_added"],
            "since": lst,
        },
        "creators": {
            "collection": db["creators"],
            "exclude_fields": ["date_added"],
            "since": lst,
        },
        "creator_roles": {
            "collection": db["creator_roles"],
            "since": lst,
        },
        "publishers": {
            "collection": db["publishers"],
            "exclude_fields": ["date_added"],
            "since": lst,
        },
        "formats": {
            "collection": db["formats"],
            "since": lst,
        },
        "languages": {
            "collection": db["languages"],
            "since": lst,
        },
        "user_badges": {
            "collection": db["user_badges"],
            "exclude_fields": ["date_added", "tiers"],
            "since": lst,
        },
        "club_badges": {
            "collection": db["club_badges"],
            "exclude_fields": ["date_added", "tiers"],
            "since": lst,
        },
        "countries": {
            "collection": db["countries"],
            "since": lst,
        },
        "users": {
            "collection": db["users"],
            "exclude_fields": excluded_user_fields,
            "field_map": user_map,
            "since": lst,
        },
        "clubs": {
            "collection": db["clubs"],
            "exclude_fields": excluded_club_fields,
            "field_map": club_map,
            "since": lst,
        },
        "user_reads": {
            "collection": db["user_reads"],
            "since": lst,
        },
        "club_period_books": {
            "collection": db["club_period_books"],
            "since": lst,
        },
        "club_reading_periods": {
            "collection": db["club_reading_periods"],
            "since": lst,
        }
    }

    return tasks


# FINAL TRANSFORMATION

def transform_collections(results):
    """Enrich and transform collections."""

    # Enrich users
    current_year = datetime.now().year
    users = results.get("users", [])
    if users:
        for user in users:
            goals = user.get("reading_goal", [])
            user["reading_goal"] = next(
                (g["goal"] for g in goals if g.get("year") == current_year),
                "N/A",
            )
            if "country" in user and user.get("key_version"):
                country = decrypt_field(user["country"], user["key_version"])
                user["country"] = country
            user.pop("key_version", None)

    # Enrich creators
    creators = results.get("creators", [])
    if creators:
        for creator in creators:
            firstname = creator.get("firstname", "")
            lastname = creator.get("lastname")
            creator["name"] = (
                f"{firstname} {lastname}".strip() if lastname else firstname
            )

    # Process books into books + book_awards
    books = results.get("books", [])
    if books:
        books, book_awards = process_books(books)
        results["books"] = books
        results["book_awards"] = book_awards

    # Process user reads
    user_reads = results.get("user_reads", [])
    if user_reads:
        user_reads = process_ur(user_reads)
        results["user_reads"] = user_reads

    # Enrich club period books
    cpb = results.get("club_period_books", [])
    crp = results.get("club_reading_periods", [])
    if cpb and crp:
        for i in cpb:
            i["period_name"] = [k["name"] for k in crp if k["_id"] == i["period_id"]][0]
        results["club_period_books"] = cpb

    return results
