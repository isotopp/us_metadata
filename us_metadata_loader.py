#! /usr/bin/python3

import json
import lzma
import pprint
import sys

import MySQLdb
import MySQLdb.cursors

db_config = dict(
    host="192.168.1.10",
    user="kris",
    password="geheim",
    db="kris",
    cursorclass=MySQLdb.cursors.DictCursor,
)

db = MySQLdb.connect(**db_config)

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
        -- citations
        -- cites_to
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
        -- preview varbinary not null,
        
        provenance_id integer not null,
        reporter_id integer not null,
        url varchar(255) not null,
        volume_id varchar(64) not null, 
        
        primary key (us_case_id)
    )""",

]


def create_tables():
    for cmd in sql_setup:
        try:
            c = db.cursor()
            c.execute(cmd)
        except MySQLdb.Error as e:
            print(f"MySQL Error: {e}", file=sys.stderr)
            sys.exit(1)


def insert(table, data, auto_increment=True):
    columns = ", ".join(data.keys())
    placeholders = [f"%({col})s" for col in data.keys()]
    valuestr = ", ".join(placeholders)

    cmd = f"""insert into {table} ({columns}) values ({valuestr})"""

    c = db.cursor()
    c.execute(cmd, data)

    if auto_increment:
        id = db.insert_id()
        return id

    return


def find_or_insert(table, data, id):
    pairs = list()
    for k in data.keys():
        pairs.append(f"{k} = %({k})s")
    condition = " and ".join(pairs)

    cmd = f"""select {id} from {table} where {condition}"""
    c = db.cursor()
    res = c.execute(cmd, data)
    if res:
        row = c.fetchone()
        id = row[id]
    else:
        id = insert(table, data)

    return id


def insert_analysis(analysis):
    pagerank = analysis.pop("pagerank", None)
    if pagerank:
        analysis["pagerank_percentile"] = pagerank["percentile"]
        analysis["pagerank_raw"] = pagerank["raw"]
    else:
        analysis["pagerank_percentile"] = 0.0
        analysis["pagerank_raw"] = 0.0

    analysis_id = insert("analysis", analysis)
    return analysis_id


def insert_citation(citation):
    citation_type = {"type": citation["type"]}
    ct_id = find_or_insert("citation_type", citation_type, "ct_id")

    citation["ct_id"] = ct_id
    citation.pop("type", None)

    return find_or_insert("citation", citation, "citation_id")


def insert_cc_rel(cc_rel):
    insert("citation_case_rel", cc_rel, auto_increment=False)


def insert_court(court):
    id = court.pop("id")
    court["court_id"] = id

    try:
        insert("court", court, auto_increment=False)
    except MySQLdb.IntegrityError as e:
        pass

    return id


def insert_jurisdiction(jurisdiction):
    id = jurisdiction.pop("id")
    jurisdiction["jurisdiction_id"] = id

    try:
        insert("jurisdiction", jurisdiction, auto_increment=False)
    except MySQLdb.IntegrityError as e:
        pass

    return id


def insert_provenance(provenance):
    id = insert("provenance", provenance)

    return id


def insert_reporter(reporter):
    id = reporter.pop("id")
    reporter["reporter_id"] = id

    try:
        insert("reporter", reporter, False)
    except MySQLdb.IntegrityError as e:
        pass

    return id


def insert_volume(volume):
    try:
        insert("volume", volume, False)
    except MySQLdb.IntegrityError as e:
        pass

    return volume["barcode"]


def insert_case(case):
    id = case.pop("id")
    case["us_case_id"] = id
    insert("us_case", case, False)

    return id


def fix_date(date):
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
with lzma.open("us_metadata_20210921/data/data.jsonl.xz", "r") as f:
    while (line := f.readline()):  # and count<3:
        d = json.loads(line)

        analysis_id = insert_analysis(d["analysis"])
        d.pop("analysis", None)
        d["analysis_id"] = analysis_id

        for citation in d["citations"]:
            citation_id = insert_citation(citation)
            cc_rel = {"case_id": d["id"], "citation_id": citation_id}
            insert_cc_rel(cc_rel)
        d.pop("citations", None)

        court_id = insert_court(d["court"])
        d.pop("court", None)
        d["court_id"] = court_id

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

        d.pop("cites_to", None)
        d.pop("preview", None)

        try:
            id = insert_case(d)
        except MySQLdb.MySQLError as e:
            print(f"Exception {e}")
            pprint.pprint(d)
            sys.exit(1)

        print(f"{count=} -- case {id=}")

        if count % 100 == 0:
            db.commit()
        count += 1
