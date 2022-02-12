#! /usr/bin/python3

import json
import lzma
import pprint
import sys

import MySQLdb
import MySQLdb.cursors

## Connection data configured here
db_config = dict(
    host="192.168.1.10",
    user="kris",
    password="geheim",
    db="kris",
    cursorclass=MySQLdb.cursors.DictCursor,
)
db = MySQLdb.connect(**db_config)


## The table structure we work with
sql_setup = [
    """drop table if exists analysis""",
    """create table analysis (
      analysis_id integer not null auto_increment,
      cardinality integer not null,
      char_count integer not null,
      ocr_confidence double not null,
      pagerank_percentile double not null,
      pagerank_raw double not null,
      random_bucket integer not null,
      random_id bigint not null,
      sha256 varchar(64) not null,
      simhash varchar(20) not null,
      word_count integer not null,
      primary key (analysis_id)
    )""",

    """drop table if exists citation_type""",
    """create table citation_type (
        ct_id integer not null auto_increment,
        type varchar(64) not null,
        
        primary key (ct_id),
        index (type)
    )""",

    """drop table if exists citation""",
    """create table citation (
        citation_id integer not null auto_increment,
        ct_id integer not null,
        cite varchar(255) not null,
        primary key (citation_id)
    )""",

    """drop table if exists citation_case_rel""",
    """create table citation_case_rel (
        citation_id integer not null,
        case_id integer not null,
        primary key (citation_id, case_id),
        index (case_id)
    )""",

    """drop table if exists court""",
    """create table court (
        court_id integer not null auto_increment,
        name varchar(255) not null,
        name_abbreviation varchar(64) not null,
        slug varchar(64) not null,
        url varchar(255) not null,
        primary key (court_id)
    )""",

    """drop table if exists jurisdiction""",
    """create table jurisdiction (
        jurisdiction_id integer not null,
        name varchar(64) not null,
        name_long varchar(255) not null,
        slug varchar(64) not null,
        url varchar(255) not null,
        whitelisted integer not null,
    
        primary key (jurisdiction_id)
    )""",

    """drop table if exists provenance""",
    """create table provenance (
        provenance_id integer not null auto_increment,
        batch varchar(64) not null,
        date_added date not null,
        source varchar(64) not null,
        
        primary key (provenance_id)
    )""",

    """drop table if exists reporter""",
    """create table reporter (
        reporter_id integer not null,
        full_name varchar(255),
        url varchar(255),

        primary key (reporter_id)
    )""",

    """drop table if exists volume""",
    """create table volume (
        barcode varchar(64) not null,
        url varchar(255) not null,
        volume_number integer not null,

        primary key (barcode)       
    )""",

    """drop table if exists us_case""",
    """create table us_case (
        us_case_id integer not null auto_increment,
        analysis_id integer not null,
        -- citations as n:m relationship
        -- cites_to ignored
        court_id integer not null,
        decision_date date not null,
        docket_number text not null,  -- varchar(255) too short
        first_page varchar(64) not null, -- "3-5"
        frontend_pdf_url text not null, -- varchar(255) too short
        frontend_url text not null, -- varchar(255) too short
        jurisdiction_id integer not null,
        last_page varchar(64) not null, -- "3-5"
        last_updated datetime not null,
        name text not null, -- varchar(255) too short
        name_abbreviation varchar(255) not null,
        -- preview ignored
        
        provenance_id integer not null,
        reporter_id integer not null,
        url varchar(255) not null,
        volume_id varchar(64) not null, 
        
        primary key (us_case_id)
    )""",

]


def create_tables():
    """ Create the table structure by executing the script in sql_setup """
    for cmd in sql_setup:
        try:
            c = db.cursor()
            c.execute(cmd)
        except MySQLdb.Error as e:
            print(f"MySQL Error: {e}", file=sys.stderr)
            sys.exit(1)


def insert(table, data, auto_increment=True):
    """
    Insert a record `data` into a table  `table`.
    If the table is using auto_increment, return the assigned id.
    If `auto_increment` is False, return nothing.

    :param table: Name of the table to insert into.
    :param data: A dict() that will be inserted. Keys are column names.
    :param auto_increment: When True, return the assigned id.
    :return: assigned id (if using auto_increment=True)
    """

    columns = ", ".join(data.keys())
    placeholders = [f"%({col})s" for col in data.keys()]
    valuestr = ", ".join(placeholders)

    # insert into table ( col1, col2, col 3) values ( %(col1)s, %(col2)s, %(col3)s )
    cmd = f"""insert into {table} ({columns}) values ({valuestr})"""

    c = db.cursor()
    c.execute(cmd, data) # data's keys must match the column names.

    if auto_increment:
        id = db.insert_id() # last_insert_id()
        return id

    return


def find_or_insert(table, data, id):
    """
    Check if the record `data` already exists in `table`. If so, return the id.
    Otherwise create the record, and return that id.

    :param table: Table to check/insert.
    :param data: A dict() with the data to insert/to check against.
    :param id: Name of the id column.
    :return: The new or found id.
    """

    pairs = list()
    for k in data.keys():
        pairs.append(f"{k} = %({k})s")
    condition = " and ".join(pairs)

    # select id from table where col1=%(col1)s and col2=%(col2)s and col3=%(col3)s
    cmd = f"""select {id} from {table} where {condition}"""
    c = db.cursor()
    res = c.execute(cmd, data)

    # res is the number of found rows, 0 or 1
    if res:
        row = c.fetchone()
        id = row[id] # return the found id
    else:
        id = insert(table, data)

    return id

##
## Table specific insert functions.
##
## Each function does the needful for that particular relation
##

def insert_analysis(analysis):
    """
    Insert into table `analysis`.

    :param analysis: The dict() to insert.
    :return: an analysis_id.
    """

    # can contain an optional sub-object with OCR quality data. We flatten it.
    pagerank = analysis.pop("pagerank", None)
    if pagerank:
        analysis["pagerank_percentile"] = pagerank["percentile"]
        analysis["pagerank_raw"] = pagerank["raw"]
    else:
        # If it is not present, we set the values to 0.0.
        analysis["pagerank_percentile"] = 0.0
        analysis["pagerank_raw"] = 0.0

    analysis_id = insert("analysis", analysis)
    return analysis_id


def insert_citation(citation):
    """
    A citation can be referenced from multiple cases. A case can reference multiple citations.
    We need a us_case <-> case_citation_rel <-> citation structure for that.

    Citations can have a type, an open-ended enumeration. We build citation_type for that.

    :param citation: A citation dict() from the current case, which can have many.
    :return: A citation_id.
    """

    # look up the citation_type (official, parallel, vendor, ...)
    citation_type = {"type": citation["type"]}
    ct_id = find_or_insert("citation_type", citation_type, "ct_id")

    citation.pop("type", None) # remove the literal type
    citation["ct_id"] = ct_id  # and put in the id instead.

    return find_or_insert("citation", citation, "citation_id")


def insert_cc_rel(cc_rel):
    """
    Insert a cc_rel dict() into the citation_case_rel table (modelling m<->n relationship).

    :param cc_rel: The dict() to insert.
    :return: None
    """
    insert("citation_case_rel", cc_rel, auto_increment=False)


def insert_court(court):
    """
    Insert a court() dict into the `court` table.

    :param court: The dict() to insert.
    :return: The court_id.
    """

    # courts come with a predefined id, which we rename to court_id.
    id = court.pop("id")
    court["court_id"] = id

    # Different cases may reference the same court, so we can ignore integrity errors
    #
    # Note how that is different from handling citations: They do not have a predefined id,
    # so we have to use find_or_insert() (comparing for full value identity, and generating ids).
    try:
        insert("court", court, auto_increment=False)
    except MySQLdb.IntegrityError as e:
        pass

    return id


def insert_jurisdiction(jurisdiction):
    """
    Insert a dict() into the jurisdiction table.

    :param jurisdiction: The dict() to insert.
    :return: The jurisdiction_id.
    """

    id = jurisdiction.pop("id")
    jurisdiction["jurisdiction_id"] = id

    # See insert_court for discussion.
    try:
        insert("jurisdiction", jurisdiction, auto_increment=False)
    except MySQLdb.IntegrityError as e:
        pass

    return id


def insert_provenance(provenance):
    """
    Insert a provenance dict() into the provenance table.

    :param provenance: The dict().
    :return: The provenance_id.
    """

    # So far no duplicates.
    id = insert("provenance", provenance)

    return id


def insert_reporter(reporter):
    """
    Insert a reporter dict() into the reporter table.

    :param reporter: The dict().
    :return: The id.
    """

    id = reporter.pop("id")
    reporter["reporter_id"] = id

    # See insert_court() for discussion.
    try:
        insert("reporter", reporter, False)
    except MySQLdb.IntegrityError as e:
        pass

    return id


def insert_volume(volume):
    """
    Insert a volume dict() into volume table.

    :param volume: The dict()
    :return: The id, which is not an integer this time, and is named volume.barcode internally.
    """

    try:
        insert("volume", volume, False)
    except MySQLdb.IntegrityError as e:
        pass

    # This is not an integer.
    return volume["barcode"]


def insert_case(case):
    """
    Insert the actual case data into the us_case table.

    :param case: The case dict().
    :return: The id.
    """

    id = case.pop("id")
    case["us_case_id"] = id
    insert("us_case", case, False)

    return id


def fix_date(date):
    """
    Some dates are incomplete 1871-01 or 1871. These are usually very old dates.
    We fix them up here so that MySQL accepts them.

    :param date: The incomplete data.
    :return: The completed date.
    """

    if len(date) == 7:
        date = f"{date}-01"
        return date

    if len(date) == 4:
        date = f"{date}-01-01"

    return date


print("**** Prepare Tables ****")
create_tables()

print("**** Load Data ****")
count = 0

# We can let Python do the decompression here. No need to have a 3G file on disk.
with lzma.open("us_metadata_20210921/data/data.jsonl.xz", "r") as f:
    while (line := f.readline()):  # and count<3:
        d = json.loads(line)

        # Extract the substructures and handle them individually
        analysis_id = insert_analysis(d["analysis"])  # store the data and assign id
        d.pop("analysis", None)                       # remove the substructure from the case
        d["analysis_id"] = analysis_id                # and add the id instead.

        # A case can have many citations, and a citation can be cited by many cases.
        for citation in d["citations"]:
            citation_id = insert_citation(citation)
            cc_rel = {"case_id": d["id"], "citation_id": citation_id}
            insert_cc_rel(cc_rel)
        d.pop("citations", None)

        court_id = insert_court(d["court"])
        d.pop("court", None)
        d["court_id"] = court_id

        # fix incomplete dates
        d["decision_date"] = fix_date(d["decision_date"])

        jurisdiction_id = insert_jurisdiction(d["jurisdiction"])
        d.pop("jurisdiction", None)
        d["jurisdiction_id"] = jurisdiction_id

        provenance_id = insert_provenance(d["provenance"])
        d.pop("provenance", None)
        d["provenance_id"] = provenance_id

        reporter_id = insert_reporter(d["reporter"])
        d.pop("reporter", None)
        d["reporter_id"] = reporter_id

        volume_id = insert_volume(d["volume"])
        d.pop("volume", None)
        d["volume_id"] = volume_id

        # unhandled and not imported so far.
        d.pop("cites_to", None)
        d.pop("preview", None)

        # If this fails, we want to see it, and the data
        # that makes debug easier
        try:
            id = insert_case(d)
        except MySQLdb.MySQLError as e:
            print(f"Exception {e}")
            pprint.pprint(d)
            sys.exit(1)

        print(f"{count=} -- case {id=}")

        # every 100 cases we write stuff to disk
        if count % 100 == 0:
            db.commit()
        count += 1

# at the end we also write stuff to disk
db.commit()
