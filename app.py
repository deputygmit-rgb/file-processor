import os
import base64
import datetime
import io
import threading
from flask import Flask, request, redirect, url_for, flash, render_template, send_file, make_response, abort, jsonify
from werkzeug.utils import secure_filename
from flask_pymongo import PyMongo
import openpyxl
from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE
import fitz  # PyMuPDF
import pdfplumber
import pytesseract
from pymongo import MongoClient
from gridfs import GridFS, NoFile
from bson import ObjectId
from PIL import Image, ImageFilter, ImageEnhance
import html
from sentence_transformers import SentenceTransformer
import numpy as np
import nltk, re
from nltk.stem import PorterStemmer
from nltk.corpus import wordnet
import faiss, json
import bson
from bson.json_util import dumps
from waitress import serve
from bson import json_util
from flask import Response
import spacy
from spacy.matcher import Matcher, PhraseMatcher
import datetime
from bson import ObjectId
from neo4j import GraphDatabase
import datetime
import pandas as pd
from bson import ObjectId
from io import BytesIO
from docx import Document
from docx.shared import Pt
import json
import math
import re
import hashlib
from flask import send_from_directory
from flask import Flask, send_file, flash, redirect, url_for
from bson import ObjectId
from flask import abort
from flask import Flask, request, render_template, jsonify
from transformers import pipeline
from keybert import KeyBERT
import math, re

app = Flask(__name__)

# 🧠 Load summarization + analysis models (load once)
summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6", device_map="auto")
ner_analyzer = pipeline("ner", grouped_entities=True, model="dslim/bert-base-NER", device_map="auto")
kw_model = KeyBERT()
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
# --------------------------- Configuration & App --------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "your_secret_key")
app.config["MONGO_URI"] = os.getenv("MONGO_URI", "mongodb://localhost:27017/dashboard_db")
app.config["UPLOAD_FOLDER"] = os.path.join(os.getcwd(), "uploads")
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024 * 1024  # 64 MB max upload size globally
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
FILES_DB = []  # Replace with your actual DB fetch logic
# Tesseract path (optional override via env)

# else system default will be used

# FAISS files
FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "faiss_hnsw.index")
FAISS_IDMAP_PATH = os.getenv("FAISS_IDMAP_PATH", "faiss_ids.json")

# Globals
index = None
id_map = []
index_lock = threading.Lock()

# Mongo
mongo = PyMongo(app)
client = MongoClient(app.config["MONGO_URI"])
db = client.get_default_database()
fs = GridFS(db)

ALLOWED_EXTENSIONS = {"xlsx", "pptx", "pdf", "png", "jpg", "jpeg", "bmp", "gif", "tiff"}


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Call2@007"))
# --- Test Neo4j connection on startup ---
try:
    with driver.session() as session:
        result = session.run("RETURN 'Neo4j connected' AS msg")
        print(result.single()["msg"])
except Exception as e:
    print("❌ Neo4j connection failed:", e)

db = client["railwaydb"]  # use your DB name
files_collection = db["files"]  # common collection for all uploads
kg_collection = db["knowledge_graph"]  # <-- Add this line
_illegal_xml_re = re.compile(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]')
ps = PorterStemmer()

# Ensure NLTK data present (wordnet) — try to download if missing
try:
    nltk.data.find("corpora/wordnet")
except LookupError:
    nltk.download("wordnet")
    nltk.download("omw-1.4")

# SentenceTransformer model (single initialization)
model = SentenceTransformer("all-MiniLM-L6-v2")
nlp = spacy.load("en_core_web_sm")
nlp.max_length = 3_000_000  # increase to comfortably handle your largest tex
matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
rail_terms = [
    # Core entities
    "railway board", "railway zone", "railway division", "railway department",
    "zonal railway", "divisional railway", "station", "yard", "loco shed",

    # Departments
    "civil engineering", "electrical engineering", "mechanical department",
    "signal department", "telecom department", "commercial department",
    "traffic department", "stores department", "personnel department",
    "accounts department", "security department", "medical department",
    "construction department", "safety department", "operations department",
    "management department",

    # Key designations
    "divisional railway manager", "chief engineer", "senior divisional engineer",
    "principal chief engineer", "principal chief commercial manager",
    "principal chief mechanical engineer", "principal chief signal and telecom engineer",
    "principal financial advisor", "principal chief personnel officer",
    "principal chief medical director", "principal chief safety officer",
    "principal chief security commissioner", "additional general manager",

    # Common functions
    "train operations", "maintenance", "infrastructure", "rolling stock",
    "ticketing", "freight booking", "material stores", "signalling", "power supply",
    "traction distribution", "locomotive", "carriage", "wagon", "overhead equipment",

    # Railway Zones
    "northern railway", "southern railway", "eastern railway", "western railway",
    "central railway", "north eastern railway", "south eastern railway",
    "northeast frontier railway", "south central railway", "south coast railway",
    "konkan railway", "east central railway", "south east central railway",
    "north western railway", "east coast railway", "north central railway",
    "south western railway", "west central railway",

    # Major Divisions (partial list; can be extended dynamically)
    "delhi division", "mumbai division", "chennai division", "howrah division",
    "secunderabad division", "bilaspur division", "bhopal division", "hubballi division",
    "jaipur division", "nagpur division", "lucknow division", "guwahati division",
    "vadodara division", "madurai division", "raipur division", "ratlam division",
    "sambalpur division", "tiruchirappalli division"
]
signal_assets = {
    "Panel Interlocking": {"category": "Signalling System", "unit": "Stations"},
    "Electronic Interlocking": {"category": "Signalling System", "unit": "Stations"},
    "Route Relay Interlocking": {"category": "Signalling System", "unit": "Stations"},
    "LED Lit Signals": {"category": "Signal Equipment", "unit": "Stations"},
    "Data Logger": {"category": "Monitoring Equipment", "unit": "Stations"},
    "Colour Light Signalling": {"category": "Signalling System", "unit": "Stations"},
    "Block Proving by Axle Counter": {"category": "Train Detection", "unit": "Block Sections"},
    "Track Circuiting": {"category": "Train Detection", "unit": "Stations"},
    "Automatic Block Signalling": {"category": "Train Control", "unit": "Rkm"},
    "Intermediate Block Signall"
    "ing": {"category": "Train Control", "unit": "Nos"},
    "Interlocked Level Crossing": {"category": "Safety Infrastructure", "unit": "Nos"},
    "Kavach": {"category": "Train Protection", "unit": "Rkm"}
}
rail_failure_dict = [
    # Signal & Telecommunication
    {"failure_code": "ACP", "failure_subcode": "ACP", "failure_desc": "ALARM CHAIN PULLING", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "ASF", "failure_subcode": "ASF", "failure_desc": "ADVANCED STARTER FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "AUTSF", "failure_subcode": "AUTSF", "failure_desc": "AUTOMATIC SIGNAL FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "AXCF", "failure_subcode": "AXCF", "failure_desc": "BLOCK AXLE COUNTER FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "BBST", "failure_subcode": "BBST", "failure_desc": "ST BLOCK BURST", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "BIF", "failure_subcode": "BIF", "failure_desc": "BLOCK INSTRUMENT FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "GSF", "failure_subcode": "GSF", "failure_desc": "GATE SIGNAL FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "HSF", "failure_subcode": "HSF", "failure_desc": "HOME SIGNAL FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "IBSF", "failure_subcode": "IBSF", "failure_desc": "INTERMEDIATE BLOCK SIGNAL FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "ICMS", "failure_subcode": "ICMS", "failure_desc": "COA/ICMS FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "PANF", "failure_subcode": "PANF", "failure_desc": "RRI/PI FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "PTF", "failure_subcode": "PTF", "failure_desc": "POINT FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "SPEF", "failure_subcode": "SPEF", "failure_desc": "SIGNAL POWER EQUIPMENT FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "SSIF", "failure_subcode": "SSIF", "failure_desc": "SSIF FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "STCC", "failure_subcode": "STCC", "failure_desc": "CABLE CUTTING", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},
    {"failure_code": "TCF", "failure_subcode": "TCF", "failure_desc": "TRACK CIRCUIT FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "SIGN AND TELE", "department": "Signal and Telecommunication"},

    # Mechanical (Coaches, Wagons, Diesel Loco)
    {"failure_code": "ACOP", "failure_subcode": "ACOP", "failure_desc": "PASSENGER TRAIN ANGLE COCK OPERATED", "valid": "Y", "user_asset_failure": "C", "system_auto": "Y", "asset_group": "MECHANICAL", "department": "Mechanical"},
    {"failure_code": "BBJP", "failure_subcode": "BBJP", "failure_desc": "PASSENGER TRAIN BRAKE BLOCK JAM", "valid": "Y", "user_asset_failure": "C", "system_auto": "Y", "asset_group": "MECHANICAL", "department": "Mechanical"},
    {"failure_code": "DLFG", "failure_subcode": "DLFG", "failure_desc": "DIESEL GOODS TRAIN LOCO FAILED", "valid": "Y", "user_asset_failure": "DL", "system_auto": "Y", "asset_group": "MECHANICAL", "department": "Mechanical"},
    {"failure_code": "DLORWP", "failure_subcode": "DLORWP", "failure_desc": "DIESEL LOCO LOSS ON RUN DUE TO WRONG POWER", "valid": "Y", "user_asset_failure": "DL", "system_auto": "Y", "asset_group": "MECHANICAL", "department": "Mechanical"},
    {"failure_code": "DLTG", "failure_subcode": "DLTG", "failure_desc": "DIESEL GOODS TRAIN LOCO TROUBLE", "valid": "Y", "user_asset_failure": "DL", "system_auto": "Y", "asset_group": "MECHANICAL", "department": "Mechanical"},

    # Electrical
    {"failure_code": "ELFG", "failure_subcode": "ELFG", "failure_desc": "ELECTRIC GOODS TRAIN LOCO FAILED", "valid": "Y", "user_asset_failure": "EL", "system_auto": "Y", "asset_group": "ELECTRICAL", "department": "Electrical"},
    {"failure_code": "OHE", "failure_subcode": "OHE", "failure_desc": "OHE FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "ELECTRICAL", "department": "Electrical"},
    {"failure_code": "ATF", "failure_subcode": "ATF", "failure_desc": "AUXILIARY TRANSFORMER FAILURE", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "ELECTRICAL", "department": "Electrical"},
    {"failure_code": "REOHE", "failure_subcode": "REOHE", "failure_desc": "OHE HANGING ETC", "valid": "N", "user_asset_failure": "", "system_auto": "Y", "asset_group": "ELECTRICAL", "department": "Electrical"},

    # Engineering
    {"failure_code": "BBENG", "failure_subcode": "BBENG", "failure_desc": "BLOCK BURSTING", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "ENGINEERING", "department": "Engineering"},
    {"failure_code": "BUCTRC", "failure_subcode": "BUCTRC", "failure_desc": "BUCKLING OF TRACK", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "ENGINEERING", "department": "Engineering"},
    {"failure_code": "TRB", "failure_subcode": "TRB", "failure_desc": "TONGUE RAIL BROKEN", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "ENGINEERING", "department": "Engineering"},

    # Law & Order
    {"failure_code": "AOR", "failure_subcode": "AOR", "failure_desc": "ATTACK ON RLY-MEN/PROPERTY/TRN", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "LAW AND ORDER", "department": "Security"},
    {"failure_code": "BNDH", "failure_subcode": "BNDH", "failure_desc": "BANDH", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "LAW AND ORDER", "department": "Security"},
    {"failure_code": "MAOTH", "failure_subcode": "MAOTH", "failure_desc": "MISCREANT ACTIVITY OTHERS", "valid": "Y", "user_asset_failure": "", "system_auto": "Y", "asset_group": "LAW AND ORDER", "department": "Security"},

    # Blocks
    {"failure_code": "BBELECN", "failure_subcode": "BBELECN", "failure_desc": "PBC ELEC CNST BLOCK BURST", "valid": "Y", "user_asset_failure": "", "system_auto": "", "asset_group": "BLOCKS", "department": "Blocks"},
]
# --------------------------- FAISS helpers --------------------------------
def format_time(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S")    
def load_faiss_index():
    global index, id_map
    if os.path.exists(FAISS_INDEX_PATH):
        try:
            index = faiss.read_index(FAISS_INDEX_PATH)
        except Exception as e:
            app.logger.exception("Failed to load FAISS index: %s", e)
            index = None
    else:
        index = None

    if os.path.exists(FAISS_IDMAP_PATH):
        try:
            with open(FAISS_IDMAP_PATH, "r", encoding="utf-8") as f:
                id_map[:] = json.load(f)
        except Exception as e:
            app.logger.exception("Failed to load id_map: %s", e)
            id_map[:] = []
    else:
        id_map[:] = []

    # Validate
    try:
        if index is not None:
            idx_dim = getattr(index, "d", None)
            app.logger.info("Loaded FAISS index dim=%s ntotal=%s", idx_dim, index.ntotal)
            if index.ntotal != len(id_map):
                app.logger.warning("FAISS ntotal (%d) != id_map length (%d). Truncating id_map.", index.ntotal, len(id_map))
                id_map[:] = id_map[: index.ntotal]
    except Exception:
        pass

# initial load
load_faiss_index()


def create_kg(tx, rail_failure_dict):
    # Create Departments and AssetGroups first
    for item in rail_failure_dict:
        department = item["department"]
        asset_group = item["asset_group"]
        failure_code = item["failure_code"]
        failure_desc = item["failure_desc"]

        # Merge ensures no duplicates
        tx.run("MERGE (d:Department {name: $department})", department=department)
        tx.run("""
            MERGE (ag:AssetGroup {name: $asset_group})
            MERGE (d:Department {name: $department})
            MERGE (d)-[:HAS_ASSET_GROUP]->(ag)
        """, department=department, asset_group=asset_group)

        # Failure node
        tx.run("""
            MERGE (f:Failure {code: $failure_code, description: $failure_desc})
            MERGE (ag:AssetGroup {name: $asset_group})
            MERGE (ag)-[:HAS_FAILURE]->(f)
            MERGE (f)-[:BELONGS_TO]->(:Department {name: $department})
        """, failure_code=failure_code, failure_desc=failure_desc, asset_group=asset_group, department=department)
with driver.session() as session:
    session.execute_write(create_kg, rail_failure_dict)

def add_failures_to_graph(failures):
    """
    Add failures to Neo4j KG.
    
    failures: List of dicts with keys:
        department, asset_type, failure_code, failure_subcode, failure_desc, valid, user_asset_failure, system_auto, asset_group
    """
    with driver.session() as session:
        for f in failures:
            session.execute_write(_create_failure_nodes, f)

def _create_failure_nodes(tx, f):
    # Merge Department node
    tx.run("""
        MERGE (d:Department {name: $department})
    """, department=f['department'])
    
    # Merge AssetType node and connect to Department
    tx.run("""
        MATCH (d:Department {name: $department})
        MERGE (a:AssetType {name: $asset_type})
        MERGE (d)-[:HAS_ASSET]->(a)
    """, department=f['department'], asset_type=f['asset_type'])
    
    # Merge FailureCode node and connect to AssetType
    tx.run("""
        MATCH (a:AssetType {name: $asset_type})
        MERGE (fc:FailureCode {code: $failure_code})
        SET fc.subcode = $failure_subcode,
            fc.description = $failure_desc,
            fc.valid = $valid,
            fc.user_asset_failure = $user_asset_failure,
            fc.system_auto = $system_auto,
            fc.asset_group = $asset_group
        MERGE (a)-[:HAS_FAILURE]->(fc)
    """, 
    asset_type=f['asset_type'], 
    failure_code=f['failure_code'], 
    failure_subcode=f.get('failure_subcode',''),
    failure_desc=f.get('failure_desc',''),
    valid=f.get('valid','Y'),
    user_asset_failure=f.get('user_asset_failure',''),
    system_auto=f.get('system_auto',''),
    asset_group=f.get('asset_group',''))



def add_to_faiss(mongo_id: str, embedding: np.ndarray, normalize: bool = True):
    """
    Add embedding numpy array to FAISS in a thread-safe and dtype-safe manner.
    By default it normalizes vectors (recommended) so cosine similarity can be computed via inner product.
    """
    global index, id_map

    vec = np.asarray(embedding, dtype="float32").reshape(1, -1)

    if normalize:
        nrm = np.linalg.norm(vec, axis=1, keepdims=True)
        nrm[nrm == 0] = 1.0
        vec = vec / nrm

    with index_lock:
        if index is None:
            dim = vec.shape[1]
            index = faiss.IndexHNSWFlat(dim, 32)
            index.hnsw.efConstruction = 200
            index.hnsw.efSearch = 50

        # check dims
        if vec.shape[1] != index.d:
            raise RuntimeError(f"Embedding dimension ({vec.shape[1]}) does not match index dimension ({index.d})")

        index.add(vec)
        id_map.append(mongo_id)

        # persist
        try:
            faiss.write_index(index, FAISS_INDEX_PATH)
            with open(FAISS_IDMAP_PATH, "w", encoding="utf-8") as f:
                json.dump(id_map, f)
        except Exception as e:
            app.logger.exception("Failed to persist FAISS index or id_map: %s", e)




def rebuild_faiss(batch_size=256, normalize=True, drop_existing=True):
    """
    Rebuild FAISS index from MongoDB documents. This re-encodes search_text using the current model.
    WARNING: This will overwrite the FAISS index and id_map files when finished.
    """
    global index, id_map

    if drop_existing:
        try:
            if os.path.exists(FAISS_INDEX_PATH):
                os.rename(FAISS_INDEX_PATH, FAISS_INDEX_PATH + ".bak")
            if os.path.exists(FAISS_IDMAP_PATH):
                os.rename(FAISS_IDMAP_PATH, FAISS_IDMAP_PATH + ".bak")
        except Exception as e:
            app.logger.warning("Failed to backup old index files: %s", e)

    cursor = mongo.db.files.find({}, {"_id": 1, "search_text": 1})
    id_map = []
    index = None
    batch_ids = []
    batch_texts = []
    total = 0

    for doc in cursor:
        batch_ids.append(str(doc["_id"]))
        batch_texts.append(doc.get("search_text", "") or "")
        if len(batch_ids) >= batch_size:
            emb = model.encode(batch_texts, convert_to_numpy=True)
            emb = np.asarray(emb, dtype="float32")
            if normalize:
                norms = np.linalg.norm(emb, axis=1, keepdims=True)
                norms[norms == 0] = 1.0
                emb = emb / norms
            with index_lock:
                if index is None:
                    dim = emb.shape[1]
                    index = faiss.IndexHNSWFlat(dim, 32)
                    index.hnsw.efConstruction = 200
                    index.hnsw.efSearch = 50
                index.add(emb)
                id_map.extend(batch_ids)
            total += len(batch_ids)
            batch_ids = []
            batch_texts = []

    if batch_ids:
        emb = model.encode(batch_texts, convert_to_numpy=True)
        emb = np.asarray(emb, dtype="float32")
        if normalize:
            norms = np.linalg.norm(emb, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            emb = emb / norms
        with index_lock:
            if index is None:
                dim = emb.shape[1]
                index = faiss.IndexHNSWFlat(dim, 32)
                index.hnsw.efConstruction = 200
                index.hnsw.efSearch = 50
            index.add(emb)
            id_map.extend(batch_ids)
        total += len(batch_ids)

    # persist
    with index_lock:
        if index is not None:
            faiss.write_index(index, FAISS_INDEX_PATH)
        with open(FAISS_IDMAP_PATH, "w", encoding="utf-8") as f:
            json.dump(id_map, f)

    app.logger.info("Rebuilt FAISS index with %d vectors.", total)
    return total

# --------------------------- Utilities & processing --------------------------------

def flatten_text(data):
    texts = []
    if data is None:
        return texts

    if isinstance(data, dict):
        for v in data.values():
            texts.extend(flatten_text(v))
    elif isinstance(data, (list, tuple)):
        for item in data:
            texts.extend(flatten_text(item))
    elif isinstance(data, str):
        texts.append(data)
    elif isinstance(data, (datetime.date, datetime.datetime)):
        texts.append(data.isoformat())
    elif isinstance(data, datetime.time):
        texts.append(data.strftime("%H:%M:%S"))
    elif isinstance(data, ObjectId):
        texts.append(str(data))
    else:
        try:
            texts.append(str(data))
        except Exception:
            pass

    return texts


import re

def make_snippet(text, query, max_words=400):
    """
    Generate a snippet from text that focuses on relevant paragraphs.
    Uses progressive AND/OR paragraph extraction to ensure snippet
    contains query terms.
    """

    if not text or not query:
        return ""

    # Step 1: Extract relevant paragraphs
    paragraphs = extract_relevant_paragraphs(text, query, max_chunks=5)

    if not paragraphs:
        # fallback: take first max_words words from full text
        words = re.findall(r'\w+', text)
        return " ".join(words[:max_words])

    # Step 2: Combine top paragraphs for snippet
    snippet_text = " ".join(paragraphs)

    # Step 3: Limit snippet length
    words = re.findall(r'\w+', snippet_text)
    snippet = " ".join(words[:max_words])

    return snippet


def ensure_text_index():
    try:
        existing = [idx["name"] for idx in db.files.list_indexes()]
        if "files_text_index" not in existing:
            db.files.create_index([("filename", "text"), ("search_text", "text")], name="files_text_index", default_language="english")
            app.logger.info("Created text index 'files_text_index' on filename + search_text")
        else:
            app.logger.info("Text index 'files_text_index' already exists")
    except Exception as e:
        app.logger.exception("Failed to create/check text index: %s", e)

ensure_text_index()


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS



_illegal_xml_re = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f]"  # illegal XML chars
)

def clean_cell_value(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return _illegal_xml_re.sub("", value)
    elif isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    elif isinstance(value, datetime.time):
        return value.strftime("%H:%M:%S")
    return str(value)




def process_excel(file_stream):
    wb = openpyxl.load_workbook(file_stream, data_only=True)
    sheets_data = {}
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.values)
        rows = [row for row in rows if any(cell not in (None, "", " ") for cell in row)]
        if not rows:
            continue
        cols = list(zip(*rows))
        non_empty_cols_idx = [i for i, col in enumerate(cols) if any(cell not in (None, "", " ") for cell in col)]
        if not non_empty_cols_idx:
            continue
        cleaned_rows = []
        for row in rows:
            cleaned_row = [row[i] if i < len(row) else None for i in non_empty_cols_idx]
            cleaned_rows.append(cleaned_row)
        sheets_data[sheet_name] = cleaned_rows
    return sheets_data


def process_pptx(file_stream):
    prs = Presentation(file_stream)
    slides_data = []
    for slide_idx, slide in enumerate(prs.slides, start=1):
        slide_texts = []
        slide_tables = []
        slide_images = []
        def extract_images_from_shape(shape):
            if shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
                image = shape.image
                image_bytes = image.blob
                image_ext = image.ext.lower() if image.ext else "png"
                try:
                    ocr_text = extract_text_from_image(image_bytes)
                except Exception:
                    ocr_text = ""
                image_id = fs.put(image_bytes, contentType=f"image/{image_ext}", filename=f"slide{slide_idx}_image.{image_ext}")
                slide_images.append({"id": str(image_id), "ext": image_ext, "ocr_text": ocr_text})
            elif shape.shape_type == MSO_SHAPE_TYPE.GROUP:
                for shp in shape.shapes:
                    extract_images_from_shape(shp)
        for shape in slide.shapes:
            if hasattr(shape, "text") and shape.text.strip():
                slide_texts.append(shape.text.strip())
            if shape.has_table:
                table = shape.table
                rows = []
                for r in range(len(table.rows)):
                    row_cells = []
                    for c in range(len(table.columns)):
                        cell_text = table.cell(r, c).text.strip()
                        row_cells.append(cell_text)
                    rows.append(row_cells)
                slide_tables.append(rows)
            extract_images_from_shape(shape)
        slides_data.append({"title": slide.shapes.title.text if slide.shapes.title else "", "texts": slide_texts, "tables": slide_tables, "images": slide_images})
    return slides_data


def preprocess_image_for_ocr(image_bytes):
    image = Image.open(io.BytesIO(image_bytes))
    image = image.convert("L")
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.0)
    image = image.filter(ImageFilter.SHARPEN)
    min_size = 300
    if image.width < min_size or image.height < min_size:
        scale = max(min_size / image.width, min_size / image.height)
        new_size = (int(image.width * scale), int(image.height * scale))
        image = image.resize(new_size, Image.LANCZOS)
    return image


def clean_extracted_text(text):
    # Remove (cid:####) patterns
    text = re.sub(r'\(cid:\d+\)', '', text)
    
    # Collapse tripled/doubled characters like "RReegguullaarr" → "Regular"
    text = re.sub(r'([A-Za-z])\1{1,}', r'\1', text)
    
    # Remove multiple spaces / line breaks
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove repeating words (basic heuristic)
    words = text.split()
    cleaned_words = []
    for w in words:
        if not cleaned_words or w.lower() != cleaned_words[-1].lower():
            cleaned_words.append(w)
    text = " ".join(cleaned_words)
    
    return text

def extract_relations(text):
    doc = nlp(text)
    triples = []
    for sent in doc.sents:
        subj = None
        obj = None
        verb = None
        for token in sent:
            if "subj" in token.dep_:
                subj = token.text
            if "obj" in token.dep_:
                obj = token.text
            if token.pos_ == "VERB":
                verb = token.lemma_
        if subj and verb and obj:
            triples.append((subj, verb, obj))
    return triples


def create_entity_node(tx, entity, file_id):
    """
    entity = {"text": "BLOCK AXLE COUNTER", "label": "FAILURE"}
    file_id = MongoDB file ID
    """
    # Store primitive properties only
    print(f"Creating entity node: {entity} for file_id: {file_id}")
    text = entity.get("text", "").strip()
    label = entity.get("label", "UNKNOWN").strip()
    if not text:
        return  # skip empty entities

    tx.run("""
        MERGE (e:Entity {text: $text, label: $label})
        MERGE (e)-[:MENTIONED_IN]->(f:File {id: $file_id})
    """, text=text, label=label, file_id=str(file_id))


def create_relation_node(tx, relation, file_id):
    """
    relation = {"from": "BLOCK AXLE COUNTER", "to": "SIGNAL PANEL", "type": "RELATED_TO"}
    """
    print(f"Creating relation node: {relation} for file_id: {file_id}")
    from_text = relation.get("from", "").strip()
    to_text = relation.get("to", "").strip()
    rel_type = relation.get("type", "RELATED_TO").strip()

    if not from_text or not to_text:
        print("⚠️ Skipping relation with missing endpoints:", relation)
        return

    # SAFEST approach: use APOC with dynamic rel type, via 'apoc.do.when' fallback
    cypher = """
    MERGE (a:Entity {text: $from_text})
    MERGE (b:Entity {text: $to_text})
    CALL apoc.create.relationship(a, $rel_type, {}, b) YIELD rel
    MERGE (a)-[:MENTIONED_IN]->(f:File {id: $file_id})
    MERGE (b)-[:MENTIONED_IN]->(f)
    RETURN rel
    """

    try:
        tx.run(cypher, from_text=from_text, to_text=to_text, rel_type=rel_type, file_id=str(file_id))
    except Exception as e:
        print(f"❌ Neo4j error while creating relation {relation}: {e}")


def add_to_graph(entities, relations, file_id):
    print("🧠 Starting KG extraction for uploaded JSON...")

    with driver.session() as session:
        # --- Add entities ---
        for e in entities:
            session.execute_write(create_entity_node, e, file_id)

        # --- Add relations ---
        for r in relations:
            # Normalize all possible shapes into standard format
            if isinstance(r, tuple) and len(r) == 3:
                r = {"from": r[0], "type": r[1], "to": r[2]}
            elif isinstance(r, dict):
                if "head" in r and "relation" in r and "tail" in r:
                    r = {"from": r["head"], "type": r["relation"], "to": r["tail"]}
                elif not all(k in r for k in ("from", "to")):
                    print(f"⚠️ Skipping malformed relation: {r}")
                    continue
            else:
                print(f"⚠️ Skipping invalid relation format: {r}")
                continue

            print("🔗 Normalized relation:", r)
            session.execute_write(create_relation_node, r, file_id)
      


def process_pdf(file_stream):
    file_stream.seek(0)
    file_bytes = file_stream.read()
    pages_data = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            text= clean_extracted_text(text)
            tables = page.extract_tables() or []
            page_fitz = doc.load_page(i)
            image_texts = []
            image_refs = []
            for img_info in page_fitz.get_images(full=True):
                
                if(len(text) < 1000):
                   xref = img_info[0]
                   base_image = doc.extract_image(xref)
                   image_bytes = base_image["image"]
                   image_ext = base_image.get("ext", "png").lower()
                   ocr_text = extract_text_from_image(image_bytes) or ""
                   ocr_text= clean_extracted_text(ocr_text)
                   if ocr_text:
                       image_texts.append(ocr_text)
                   image_id = fs.put(image_bytes, contentType=f"image/{image_ext}", filename=f"page{i+1}_image.{image_ext}")
                   image_refs.append({"id": str(image_id), "ext": image_ext, "ocr_text": ocr_text})
            if not text.strip() and len(text) < 1000:
                pix = page_fitz.get_pixmap(dpi=300)
                img_bytes = pix.tobytes("png")
                ocr_text = extract_text_from_image(img_bytes, lang="eng")
                ocr_text= clean_extracted_text(ocr_text)
                if ocr_text:
                    text = ocr_text
                    image_texts.append(ocr_text)
            pages_data.append({"page_number": i + 1, "text": text, "tables": tables, "image_texts": image_texts, "images": image_refs})
        doc.close()
    return pages_data


def check_tesseract_available():
    try:
        version = pytesseract.get_tesseract_version()
        print(f"Tesseract available: {version}")
    except Exception as e:
        raise RuntimeError(f"Tesseract is not functioning: {e}")


def extract_text_from_image(image_bytes, lang="eng"):
    try:
        image = preprocess_image_for_ocr(image_bytes)
        text = pytesseract.image_to_string(image, lang=lang, config="--psm 6").strip()
        text= clean_extracted_text(text)
        if not text:
            text = pytesseract.image_to_string(image, lang=lang, config="--psm 3").strip()
        return text
    except Exception as e:
        app.logger.warning(f"OCR failed: {e}")
        return ""


def process_image(file_stream, filename):
    file_stream.seek(0)
    image_bytes = file_stream.read()
    text = extract_text_from_image(image_bytes)
    text= clean_extracted_text(text)
    file_stream.seek(0)
    image_ext = filename.rsplit(".", 1)[1].lower()
    image_id = fs.put(image_bytes, contentType=f"image/{image_ext}", filename=filename)
    data = {"images": [{"id": str(image_id), "ext": image_ext}], "ocr_text": text}
    return data

def clean_for_mongo(obj):
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, datetime.time):
        return obj.strftime("%H:%M:%S")
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        return {k: clean_for_mongo(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean_for_mongo(x) for x in obj]
    try:
        return str(obj)
    except Exception:
        return None


def compute_file_hash(file_input):
    hasher = hashlib.sha256()
    if isinstance(file_input, (bytes, bytearray)):
        hasher.update(file_input)
    else:
        for chunk in iter(lambda: file_input.read(4096), b""):
            hasher.update(chunk)
        file_input.seek(0)
    return hasher.hexdigest()

def process_and_store_file(abs_path):
    filename = os.path.basename(abs_path)
    filetype = filename.rsplit(".", 1)[1].lower()
    with open(abs_path, "rb") as f:
        file_bytes = f.read()

    # Save in GridFS
    file_id = fs.put(file_bytes, filename=filename, contentType="application/octet-stream")

    # Process file
    if filetype == "xlsx":
        file_data = process_excel(io.BytesIO(file_bytes))
    elif filetype == "pptx":
        file_data = process_pptx(io.BytesIO(file_bytes))
    elif filetype == "pdf":
        file_data = process_pdf(io.BytesIO(file_bytes))
    elif filetype in {"png", "jpg", "jpeg", "bmp", "gif", "tiff"}:
        file_data = process_image(io.BytesIO(file_bytes), filename)
    else:
        raise ValueError("Unsupported file type")

    # Build search text
    _search_tokens = flatten_text(file_data)
    _search_tokens.insert(0, filename)
    search_text = " ".join([str(t) for t in _search_tokens if t])

    # Generate embedding
    embedding = model.encode(search_text, convert_to_numpy=True).astype("float32")
    

    # Railway domain terms
    
    cleaned_data = clean_for_mongo(file_data)
    inserted = mongo.db.files.insert_one({
        "filename": filename,
        "filetype": filetype,
        "file_id": file_id,
        "search_text": search_text,
        "embedding": embedding.tolist(),
        "upload_time": datetime.datetime.utcnow(),
        "data": cleaned_data
    })
    rail_terms = ["wagon", "hopper", "open wagon", "loading", "unloading",
              "demurrage", "freight", "free time", "station", "railway board"]

    
    patterns = [nlp.make_doc(t) for t in rail_terms]
    matcher.add("RAIL_TERMS", patterns)
    # Add to FAISS
    add_to_faiss(str(inserted.inserted_id), embedding)

    return inserted.inserted_id, filename

def extract_entities(text, chunk_size=100_000):
    entities = []
    n = len(text)
    for i in range(0, n, chunk_size):
        chunk = text[i:i+chunk_size]
        doc = nlp(chunk)
        for ent in doc.ents:
            entities.append({"text": ent.text, "label": ent.label_})
    return entities


def process_and_store_file_with_check(abs_path):
    filename = os.path.basename(abs_path)

    with open(abs_path, "rb") as f:
        file_bytes = f.read()

    file_hash = compute_file_hash(file_bytes)

    # 🔹 Check if already in Mongo
    existing = mongo.db.files.find_one({"file_hash": file_hash})
    if existing:
        # Return existing without re-processing
        return existing["_id"], existing["filename"]

    # 🔹 Call your original function
    inserted_id, stored_filename = process_and_store_file(abs_path)

    # Update with hash for future checks
    mongo.db.files.update_one(
        {"_id": inserted_id},
        {"$set": {"file_hash": file_hash}}
    )

    return inserted_id, stored_filename

def extract_signal_assets(text):
    assets = []
    for name in signal_assets.keys():
        pattern = rf"{re.escape(name)}.*?(\d[\d,]*)"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            count = int(match.group(1).replace(",", ""))
            assets.append({
                "asset_name": name,
                "count": count,
                "category": signal_assets[name]["category"],
                "unit": signal_assets[name]["unit"],
                "as_on": "2025-08-31"
            })
    return assets


def check_duplicate_and_save(file, files_collection, extra_metadata=None):
    """
    Check if file already exists in MongoDB based on hash.
    If not, insert new metadata.
    Returns: (status, existing_or_new_doc)
    """
    file_hash = compute_file_hash(file.stream)

    # Look for existing entry
    existing = files_collection.find_one({"file_hash": file_hash})
    if existing:
        return "duplicate", existing

    # Insert new document metadata
    new_doc = {
        "filename": file.filename,
        "file_hash": file_hash,
        "content_type": file.content_type,
        "uploaded_at": datetime.datetime.utcnow()
    }
    if extra_metadata:
        new_doc.update(extra_metadata)

    inserted_id = files_collection.insert_one(new_doc).inserted_id
    new_doc["_id"] = inserted_id
    return "new", new_doc

def add_assets_to_graph(assets, department, directorate):
    with driver.session() as session:
        for a in assets:
            session.run("""
                MERGE (dept:Department {name: $dept})
                MERGE (dir:Directorate {name: $dir})
                MERGE (dept)-[:HAS_DIRECTORATE]->(dir)
                MERGE (asset:Asset {name: $name})
                SET asset.count = $count, asset.unit = $unit, asset.as_on = $as_on
                MERGE (dir)-[:MAINTAINS]->(asset)
            """, {
                "dept": department,
                "dir": directorate,
                "name": a["asset_name"],
                "count": a["count"],
                "unit": a["unit"],
                "as_on": a["as_on"]
            })



def extract_failure_codes(file_data):
    """
    Extracts failure codes & details from text, dict, or Excel table.
    
    Returns a list of dictionaries:
        [
            {
                "department": "Signal and Telecommunication",
                "asset_type": "Panel Interlocking",
                "failure_code": "ACC",
                "failure_subcode": "1.01",
                "failure_desc": "ACCIDENT OTHERS",
                "valid": "Y",
                "user_asset_failure": "Y/N",
                "system_auto": "Y/N",
                "asset_group": "UNUSUAL"
            },
            ...
        ]
    """
    failures = []

    # If already a DataFrame (Excel)
    if isinstance(file_data, pd.DataFrame):
        df = file_data
    # If a dict (likely returned from process_excel/pdf)
    elif isinstance(file_data, dict):
        # Try to find a table inside dict
        if "table" in file_data and isinstance(file_data["table"], list):
            df = pd.DataFrame(file_data["table"])
        else:
            # Flatten dict to string
            text = " ".join(str(v) for v in file_data.values())
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            if not lines:
                return failures
            headers = re.split(r'\t+|\s{2,}', lines[0])
            rows = [re.split(r'\t+|\s{2,}', l) for l in lines[1:]]
            df = pd.DataFrame(rows, columns=headers)
    # If raw text
    elif isinstance(file_data, str):
        lines = [line.strip() for line in file_data.splitlines() if line.strip()]
        if not lines:
            return failures
        headers = re.split(r'\t+|\s{2,}', lines[0])
        rows = [re.split(r'\t+|\s{2,}', l) for l in lines[1:]]
        df = pd.DataFrame(rows, columns=headers)
    else:
        return failures

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    for _, row in df.iterrows():
        failure = {
            "department": row.get("department", "").strip() if row.get("department") else "",
            "asset_type": row.get("asset_type", "").strip() if row.get("asset_type") else "",
            "failure_code": row.get("failure_code", "").strip() if row.get("failure_code") else "",
            "failure_subcode": row.get("failure_sub_code", row.get("sub_sr", "")).strip() if row.get("failure_sub_code") or row.get("sub_sr") else "",
            "failure_desc": row.get("failure_description", "").strip() if row.get("failure_description") else "",
            "valid": row.get("valid?", "Y").strip() if row.get("valid?") else "Y",
            "user_asset_failure": row.get("user_asset_failure", "").strip() if row.get("user_asset_failure") else "",
            "system_auto": row.get("system_auto", "").strip() if row.get("system_auto") else "",
            "asset_group": row.get("asset_group", "").strip() if row.get("asset_group") else ""
        }
        if failure["failure_code"]:
            failures.append(failure)

    return failures


def create_failure_kg_node(tx, failure, file_id):
    tx.run("""
        MERGE (f:Failure {code: $failure_code})
        SET f.subcode = $failure_subcode,
            f.description = $failure_desc,
            f.department = $department,
            f.asset_group = $asset_group,
            f.valid = $valid,
            f.system_auto = $system_auto,
            f.file_id = $file_id
    """, 
    failure_code=failure["failure_code"],
    failure_subcode=failure.get("failure_subcode", ""),
    failure_desc=failure.get("failure_desc", ""),
    department=failure.get("department", ""),
    asset_group=failure.get("asset_group", ""),
    valid=failure.get("valid", ""),
    system_auto=failure.get("system_auto", ""),
    file_id=str(file_id))



def create_asset_kg_node(tx, asset_name, file_id):
    tx.run("""
        MERGE (a:Asset {name: $asset_name})
        SET a.file_id = $file_id
    """, asset_name=asset_name, file_id=str(file_id))



def create_relation_node(tx, relation, file_id):
    """Insert entity relations safely, handling tuple and dict types."""
    
    # --- Normalize relation type ---
    if isinstance(relation, tuple):
        if len(relation) == 3:
            from_text, rel_type, to_text = relation
        elif len(relation) == 2:  # fallback if only from & to provided
            from_text, to_text = relation
            rel_type = "RELATED_TO"
        else:
            return
    elif isinstance(relation, dict):
        from_text = relation.get("from", "").strip()
        to_text = relation.get("to", "").strip()
        rel_type = relation.get("type", "RELATED_TO").strip()
    else:
        return  # skip if not tuple/dict
    
    if not from_text or not to_text:
        return

    # --- Create/merge nodes and relation ---
    tx.run(f"""
        MERGE (a:Entity {{text: $from_text}})
        MERGE (b:Entity {{text: $to_text}})
        MERGE (a)-[r:{rel_type}]->(b)
        MERGE (a)-[:MENTIONED_IN]->(f:File {{id: $file_id}})
        MERGE (b)-[:MENTIONED_IN]->(f)
    """, from_text=from_text, to_text=to_text, file_id=str(file_id))



# --------------------------- Routes --------------------------------


@app.route("/", methods=["GET"])
def dashboard():
    files = list(mongo.db.files.find().sort("upload_time", -1).limit(15))
    selected_file_id = request.args.get("file_id", "")
    return render_template(
        "dashboard.html",
        files=files,
        selected_file_id=selected_file_id
    )


@app.route('/upload_json', methods=['POST'])
def upload_json():
    total_uploaded = 0
    total_failed = 0
    skipped_files = []

    try:
        json_file = request.files.get('file')
        if not json_file:
            flash("No JSON file uploaded", "danger")
            return redirect(url_for('dashboard', _external=True))

        data = json.load(json_file)

        for xlsx_file, file_list in data.items():
            # Only process PDFs
            pdf_files = [f for f in file_list if f.strip().lower().endswith('.pdf')]

            if not pdf_files:
                skipped_files.append(f"{xlsx_file} (no PDF files)")
                continue

            for file_path in pdf_files:
                abs_path = os.path.normpath(os.path.join(os.getcwd(), file_path))
                print(f"Trying to upload: {abs_path}")
                if not os.path.exists(abs_path):
                    skipped_files.append(f"{file_path} (not found)")
                    total_failed += 1
                    continue

                try:
                    file_id, file_name = process_and_store_file_with_check(abs_path)
                    file_doc = mongo.db.files.find_one({"_id": file_id})
                    text_content = " ".join(flatten_text(file_doc["data"]))
                    entities = extract_entities(text_content)
                    triples = extract_relations(text_content)
                    add_to_graph(entities, triples, file_id)
                    flash(f"Uploaded: {file_name}", "success")
                    total_uploaded += 1
                except Exception as e:
                    skipped_files.append(f"{file_path} (error: {str(e)})")
                    total_failed += 1

        # Flash summary
        summary_msg = f"✅ {total_uploaded} PDF files uploaded"
        if total_failed:
            summary_msg += f", ❌ {total_failed} failed"
        if skipped_files:
            summary_msg += f". Skipped: {len(skipped_files)} files"

        flash(summary_msg, "info")

    except Exception as e:
        flash(f"Error reading JSON: {str(e)}", "danger")

    return redirect(url_for('dashboard'))

@app.route("/dashboard/view/<file_id>")
def view_file_in_dashboard(file_id):
    file = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    if not file:
        flash("File not found", "danger")
        return redirect(url_for("dashboard"))

    # Pass the selected file id to the dashboard so it auto-opens
    return redirect(url_for("dashboard", selected=file_id))


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        if "file" not in request.files:
            flash("No file part", "danger")
            return redirect(request.url)

        file = request.files["file"]
        if not file or file.filename == "":
            flash("No selected file", "danger")
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filetype = filename.rsplit(".", 1)[1].lower()

            # --- Check duplicate ---
            status, doc = check_duplicate_and_save(file, files_collection, extra_metadata={"type": filetype})
            if status == "duplicate":
                return jsonify({
                    "status": "duplicate",
                    "message": "File already uploaded",
                    "file_id": str(doc["_id"]),
                    "filename": doc["filename"]
                }), 200

            file.seek(0)
            file_id = fs.put(file, filename=filename, contentType=file.mimetype)
            file.seek(0)
            try:
                if filetype in {"xlsx", "xls"}:
                    file_data = process_excel(file)
                elif filetype == "pdf":
                    file_data = process_pdf(file)
                elif filetype in {"png", "jpg", "jpeg"}:
                    file_data = process_image(file, filename)
                else:
                    flash("Unsupported file type", "danger")
                    return redirect(request.url)
            except Exception as e:
                flash(f"Failed to process file: {e}", "danger")
                return redirect(request.url)

            # --- Flatten text for search & embeddings ---
            _search_tokens = flatten_text(file_data)
            _search_tokens.insert(0, filename)
            search_text = " ".join([str(t) for t in _search_tokens if t])

            embedding = model.encode(search_text, convert_to_numpy=True)
            embedding = np.asarray(embedding, dtype="float32")

            cleaned_data = clean_for_mongo(file_data)

            # --- Extract failures & assets ---
            failures = extract_failure_codes(file_data)
            assets = [f for f in failures if f.get("user_asset_failure","Y") == "Y"]

            # --- Insert file into MongoDB ---
            inserted = mongo.db.files.insert_one({"filename": filename, "filetype": filetype, "file_id": file_id, "search_text": search_text, "embedding": embedding.tolist(), "upload_time": datetime.datetime.utcnow(), "data": cleaned_data})


            file_id = inserted.inserted_id
            try:
                add_to_faiss(str(file_id), embedding, normalize=True)
            except Exception as e:
                app.logger.exception("Failed to add to FAISS: %s", e)
            # --- Insert failures & assets into Neo4j KG ---
            with driver.session() as session:
                for failure in failures:
                    session.execute_write(create_failure_kg_node, failure, file_id)
                for asset in assets:
                    session.execute_write(create_asset_kg_node, asset, file_id)

            # --- Extract entities & relations from text and add to KG ---
            entities = extract_entities(search_text)
            relations  = extract_relations(search_text)
            try:
                add_to_graph(entities, relations, file_id)
            except Exception as e:
                app.logger.exception(f"Failed to add KG entities/relations: {e}")

            print(f"Added {len(entities)} entities and {len(relations)} relations to KG")
            # --- Add to FAISS ---
            

            flash("File uploaded, processed & indexed successfully", "success")
            return redirect(url_for("dashboard"))

        flash("Invalid file type. Allowed: .xlsx, .pdf, images", "danger")
        return redirect(request.url)

    return render_template("upload_file.html")

@app.route("/files/<file_id>")
def serve_file(file_id):
    try:
        grid_out = fs.get(ObjectId(file_id))
        return send_file(io.BytesIO(grid_out.read()), download_name=grid_out.filename, mimetype=grid_out.content_type or "application/octet-stream", as_attachment=True)
    except NoFile:
        flash("File not found in storage.", "danger")
        return redirect(url_for("dashboard"))


@app.route("/images/<image_id>")
def serve_image(image_id):
    try:
        grid_out = fs.get(ObjectId(image_id))
        mimetype = grid_out.content_type or "application/octet-stream"
        return send_file(io.BytesIO(grid_out.read()), download_name=grid_out.filename, mimetype=mimetype, as_attachment=False)
    except NoFile:
        abort(404)


@app.route("/export_json/<file_id>")
def export_json(file_id):
    file_doc = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    if not file_doc or "data" not in file_doc:
        flash("File or data not found.", "danger")
        return redirect(url_for("dashboard"))

    def json_serializer(obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return str(obj)

    json_data = json.dumps(file_doc["data"], indent=2, ensure_ascii=False, default=json_serializer)
    response = make_response(json_data)
    response.headers["Content-Disposition"] = f"attachment; filename={file_doc['filename']}_data.json"
    response.headers["Content-Type"] = "application/json"
    return response


def write_excel_from_data(file_name_prefix, filetype, data):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    if filetype == "xlsx":
        for sheet_name, rows in data.items():
            ws = wb.create_sheet(title=sheet_name[:31])
            for row_idx, row in enumerate(rows, 1):
                for col_idx, cell in enumerate(row, 1):
                    ws.cell(row=row_idx, column=col_idx, value=clean_cell_value(cell))
    elif filetype == "pptx":
        for slide_idx, slide in enumerate(data, 1):
            ws = wb.create_sheet(title=f"Slide {slide_idx}")
            row_cursor = 1
            ws.cell(row=row_cursor, column=1, value="Title")
            ws.cell(row=row_cursor, column=2, value=clean_cell_value(slide.get("title", "")))
            row_cursor += 2
            ws.cell(row=row_cursor, column=1, value="Texts")
            row_cursor += 1
            for text in slide.get("texts", []):
                ws.cell(row=row_cursor, column=1, value=clean_cell_value(text))
                row_cursor += 1
            row_cursor += 1
            images = slide.get("images", [])
            if images:
                ws.cell(row=row_cursor, column=1, value=f"Images ({len(images)} stored in DB)")
                row_cursor += 2
                for img_idx, img in enumerate(images, 1):
                    ws.cell(row=row_cursor, column=1, value=f"Image {img_idx} (id: {img['id']}, ext: .{img.get('ext', 'png')})")
                    row_cursor += 1
                    if img.get("ocr_text"):
                        for line in img["ocr_text"].splitlines():
                            ws.cell(row=row_cursor, column=2, value=clean_cell_value(line))
                            row_cursor += 1
                    row_cursor += 1
            for t_idx, table in enumerate(slide.get("tables", []), 1):
                ws.cell(row=row_cursor, column=1, value=f"Table {t_idx}")
                row_cursor += 1
                for row in table:
                    for col_idx, cell in enumerate(row, 1):
                        ws.cell(row=row_cursor, column=col_idx, value=clean_cell_value(cell))
                    row_cursor += 1
                row_cursor += 1
    elif filetype == "pdf":
        for page in data:
            ws = wb.create_sheet(title=f"Page {page.get('page_number', '?')}")
            row_cursor = 1
            ws.cell(row=row_cursor, column=1, value="Extracted Text")
            row_cursor += 1
            for line in page.get("text", "").splitlines():
                ws.cell(row=row_cursor, column=1, value=clean_cell_value(line))
                row_cursor += 1
            row_cursor += 1
            for t_idx, table in enumerate(page.get("tables", []), 1):
                ws.cell(row=row_cursor, column=1, value=f"Table {t_idx}")
                row_cursor += 1
                for row in table:
                    for col_idx, cell in enumerate(row, 1):
                        ws.cell(row=row_cursor, column=col_idx, value=clean_cell_value(cell))
                    row_cursor += 1
                row_cursor += 1
            if page.get("image_texts"):
                ws.cell(row=row_cursor, column=1, value="OCR Texts from Images (aggregated)")
                row_cursor += 1
                for text in page["image_texts"]:
                    for line in text.splitlines():
                        ws.cell(row=row_cursor, column=1, value=clean_cell_value(line))
                        row_cursor += 1
                    row_cursor += 1
            images = page.get("images", [])
            if images:
                ws.cell(row=row_cursor, column=1, value=f"Images ({len(images)} stored in DB)")
                row_cursor += 2
                for img_idx, img in enumerate(images, 1):
                    ws.cell(row=row_cursor, column=1, value=f"Image {img_idx} (id: {img['id']}, ext: .{img.get('ext', 'png')})")
                    row_cursor += 1
                    if img.get("ocr_text"):
                        for line in img["ocr_text"].splitlines():
                            ws.cell(row=row_cursor, column=2, value=clean_cell_value(line))
                            row_cursor += 1
                    row_cursor += 1
    elif filetype in {"png", "jpg", "jpeg", "bmp", "gif", "tiff"}:
        ws = wb.create_sheet(title="Image Data")
        ws.cell(row=1, column=1, value="OCR Extracted Text")
        ws.cell(row=2, column=1, value=clean_cell_value(data.get("ocr_text", "")))
        ws.cell(row=4, column=1, value="Image Reference")
        images = data.get("images", [])
        for idx, img in enumerate(images, start=1):
            ws.cell(row=4 + idx, column=1, value=f"Image {idx} (id: {img['id']}, ext: .{img.get('ext','png')})")
            if img.get("ocr_text"):
                ws.cell(row=4 + idx, column=2, value=clean_cell_value(img["ocr_text"]))
    else:
        ws = wb.create_sheet(title="Data")
        ws.cell(row=1, column=1, value=clean_cell_value(json.dumps(data, indent=2)))
    excel_stream = io.BytesIO()
    wb.save(excel_stream)
    excel_stream.seek(0)
    return excel_stream


def write_word_from_data(file_name_prefix, filetype, data):
    doc = Document()

    def add_paragraph(text, bold=False, italic=False, size=11):
        p = doc.add_paragraph()
        run = p.add_run(str(text))
        run.bold = bold
        run.italic = italic
        run.font.size = Pt(size)

    if filetype == "xlsx":
        for sheet_name, rows in data.items():
            doc.add_heading(sheet_name, level=1)
            for row in rows:
                add_paragraph(" | ".join(str(cell) for cell in row))
            doc.add_page_break()

    elif filetype == "pptx":
        for slide_idx, slide in enumerate(data, 1):
            doc.add_heading(f"Slide {slide_idx}: {slide.get('title', '')}", level=1)

            if slide.get("texts"):
                doc.add_heading("Texts:", level=2)
                for text in slide["texts"]:
                    add_paragraph(text)

            images = slide.get("images", [])
            if images:
                doc.add_heading(f"Images ({len(images)} stored in DB):", level=2)
                for img_idx, img in enumerate(images, 1):
                    add_paragraph(f"Image {img_idx} (id: {img['id']}, ext: .{img.get('ext','png')})")
                    if img.get("ocr_text"):
                        for line in img["ocr_text"].splitlines():
                            add_paragraph(line)

            tables = slide.get("tables", [])
            for t_idx, table in enumerate(tables, 1):
                doc.add_heading(f"Table {t_idx}:", level=2)
                for row in table:
                    add_paragraph(" | ".join(str(cell) for cell in row))

            doc.add_page_break()

    elif filetype == "pdf":
        for page in data:
            doc.add_heading(f"Page {page.get('page_number','?')}", level=1)
            doc.add_heading("Extracted Text:", level=2)
            for line in page.get("text", "").splitlines():
                add_paragraph(line)

            for t_idx, table in enumerate(page.get("tables", []), 1):
                doc.add_heading(f"Table {t_idx}:", level=2)
                for row in table:
                    add_paragraph(" | ".join(str(cell) for cell in row))

            if page.get("image_texts"):
                doc.add_heading("OCR Texts from Images:", level=2)
                for text in page["image_texts"]:
                    for line in text.splitlines():
                        add_paragraph(line)

            images = page.get("images", [])
            if images:
                doc.add_heading(f"Images ({len(images)} stored in DB):", level=2)
                for img_idx, img in enumerate(images, 1):
                    add_paragraph(f"Image {img_idx} (id: {img['id']}, ext: .{img.get('ext','png')})")
                    if img.get("ocr_text"):
                        for line in img["ocr_text"].splitlines():
                            add_paragraph(line)

            doc.add_page_break()

    elif filetype in {"png", "jpg", "jpeg", "bmp", "gif", "tiff"}:
        doc.add_heading("Image Data", level=1)
        add_paragraph("OCR Extracted Text:", bold=True)
        add_paragraph(data.get("ocr_text", ""))

        images = data.get("images", [])
        if images:
            doc.add_heading("Image References:", level=2)
            for idx, img in enumerate(images, 1):
                add_paragraph(f"Image {idx} (id: {img['id']}, ext: .{img.get('ext','png')})")
                if img.get("ocr_text"):
                    add_paragraph(img["ocr_text"])

    else:
        doc.add_heading("Data", level=1)
        add_paragraph(json.dumps(data, indent=2))

    # Save to BytesIO stream
    word_stream = BytesIO()
    doc.save(word_stream)
    word_stream.seek(0)
    return word_stream

def expand_query(query):
    tokens = [re.sub(r"[^\w]", "", t.lower()) for t in query.split()]
    expanded = set(tokens)
    for token in tokens:
        if not token:
            continue
        expanded.add(ps.stem(token))
        for syn in wordnet.synsets(token):
            for lemma in syn.lemmas():
                expanded.add(lemma.name().replace("_", " ").lower())
    return list(expanded)


def regex_fallback(query):
    return list(mongo.db.files.find({"$or": [{"filename": {"$regex": query, "$options": "i"}}, {"search_text": {"$regex": query, "$options": "i"}}]}))

# Load FAISS index already called above




def get_failures_by_keyword(keyword):
    with driver.session() as session:
        result = session.run("""
            MATCH (f:Failure)-[:BELONGS_TO]->(d)
            WHERE f.description CONTAINS $keyword
            RETURN f.code, f.description, d.name
        """, keyword=keyword)
        return [record.data() for record in result]



def get_kg_matches(query_terms):
    """
    Fetch KG matches for the given query terms.
    Returns a list of dicts containing failure & asset info.
    """
    kg_matches = []

    with driver.session() as session:
        for term in query_terms:
            term_lower = term.lower()

            # 1️⃣ Match failures by code or description
            results_failures = session.run("""
                MATCH (f:Failure)-[:BELONGS_TO]->(d:Department)
                WHERE toLower(f.description) CONTAINS toLower($term)
                   OR toLower(f.code) = toLower($term)
                RETURN f.code AS code, f.description AS description, d.name AS department
            """, {"term": term})

            for r in results_failures:
                kg_matches.append({
                    "failure_code": r["code"],
                    "failure_desc": r["description"],
                    "department": r["department"]
                })

            # 2️⃣ Match assets safely
            results_assets = session.run("""
                MATCH (a:Asset)-[:MONITORED_BY]->(f:Failure)
                WHERE a.asset_name IS NOT NULL AND toLower(a.asset_name) CONTAINS toLower($term)
                RETURN a.asset_name AS asset_name, f.code AS failure_code
            """, {"term": term})


            for r in results_assets:
                kg_matches.append({
                    "asset_name": r["asset_name"],
                    "failure_code": r["failure_code"]
                })

    return kg_matches

def intelligent_search(query):
    sem_results = semantic_search(query, top_k=10)
    ents = extract_entities(query)
    
    kg_docs = []
    if ents:
        kg_docs = list(kg_collection.find({"entities": {"$in": ents}}))
        kg_doc_ids = [k["file_id"] for k in kg_docs]
        kg_files = list(mongo.db.files.find({"_id": {"$in": [ObjectId(fid) for fid in kg_doc_ids]}}))
    else:
        kg_files = []

    # Combine and rank
    combined = {str(r["_id"]): r for r in sem_results}
    for f in kg_files:
        fid = str(f["_id"])
        if fid not in combined:
            combined[fid] = f
            combined[fid]["semantic_score"] = 0.5  # boost from KG

    return sorted(combined.values(), key=lambda x: x.get("semantic_score", 0), reverse=True)

def semantic_search(query, top_k=10, normalize=True):
    """
    Query FAISS and return MongoDB docs sorted by descending similarity (higher is better when normalized).
    """
    global index, id_map
    if index is None or getattr(index, "ntotal", 0) == 0:
        return []

    if not query or not query.strip():
        return []

    q_vec = model.encode(query, convert_to_numpy=True)
    q_vec = np.asarray(q_vec, dtype="float32").reshape(1, -1)

    if normalize:
        qn = np.linalg.norm(q_vec, axis=1, keepdims=True)
        if qn[0][0] == 0.0:
            return []
        q_vec = q_vec / qn

    # sanity check for dimension mismatch
    if q_vec.shape[1] != index.d:
        app.logger.error("Query embedding dim (%d) != index.d (%d).", q_vec.shape[1], index.d)
        return []

    with index_lock:
        k = min(top_k, max(1, index.ntotal))
        distances, indices = index.search(q_vec, k)

    # collect valid ids (avoid __deleted__)
    valid_pairs = []
    for score, idx in zip(distances[0], indices[0]):
        if idx < 0 or idx >= len(id_map):
            continue
        mongo_id = id_map[idx]
        if mongo_id == "__deleted__":
            continue
        valid_pairs.append((float(score), mongo_id))

    if not valid_pairs:
        return []

    # batch lookup to reduce MongoDB round-trips
    mongo_ids = [ObjectId(mid) for _, mid in valid_pairs]
    found_docs = list(mongo.db.files.find({"_id": {"$in": mongo_ids}}))
    found_map = {str(doc["_id"]): doc for doc in found_docs}

    results = []
    for score, mid in valid_pairs:
        doc = found_map.get(mid)
        if doc:
            doc["semantic_score"] = score
            doc["_id_str"] = str(doc["_id"])
            results.append(doc)

    results.sort(key=lambda x: x.get("semantic_score", 0.0), reverse=True)
    return results

# 🧠 Load summarization + analysis models (load once)

def run_search(query, limit=800):
    words = [w for w in query.split() if w]
    if not words:
        return [], "empty query"

    # Projection for fields you want to return
    projection = {
        "score": {"$meta": "textScore"},
        "filename": 1,
        "filetype": 1,
        "data": 1
    }

    # Exact phrase search
    phrase_results = list(
        mongo.db.files.find({"$text": {"$search": f'"{query}"'}}, projection)
        .sort([("score", {"$meta": "textScore"})])
        .limit(limit)
    )
    if phrase_results:
        return phrase_results, "exact phrase"

    # AND search for individual words (all must appear)
    and_query = " ".join([f'"{w}"' for w in words])
    and_results = list(
        mongo.db.files.find({"$text": {"$search": and_query}}, projection)
        .sort([("score", {"$meta": "textScore"})])
        .limit(limit)
    )
    if and_results:
        return and_results, "AND match"

    return [], "no match"

import nltk
from nltk import pos_tag, word_tokenize
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt_tab')
nltk.download('averaged_perceptron_tagger_eng')
# Make sure you’ve downloaded NLTK data once in your environment:
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')

stop_words = set(stopwords.words('english'))

def refine_query_terms(query: str) -> list:
    """
    Extract domain-friendly key terms from query:
    - Keeps nouns, proper nouns, adjectives
    - Keeps codes, hyphenated and slash-separated terms
    - Filters stopwords and punctuation
    """
    query = (query or "").strip()
    if not query:
        return []

    # Tokenize
    tokens = word_tokenize(query)

    key_terms = []
    for token in tokens:
        w_clean = token.strip()
        w_lower = w_clean.lower()
        # Skip stopwords and empty strings
        if not w_clean or w_lower in stop_words:
            continue
        # Keep alphanumerics, hyphens, slashes, and uppercase codes
        if re.match(r"^[a-zA-Z0-9-/]+$", w_clean):
            key_terms.append(w_clean)
        else:
            # Optionally, keep proper nouns (POS NN/NNP) if using pos_tag
            pass

    # Fallback: keep all non-stopword tokens
    if not key_terms:
        key_terms = [t for t in tokens if t.lower() not in stop_words]

    print("Refined query terms:", key_terms)
    return key_terms

# 🧩 --- Summarization helper ---
from itertools import combinations
import re



def tokenize(text):
    """Return a set of lowercase words in the text."""
    return set(re.findall(r'\b\w+\b', text.lower()))

def summarize_relevant_chunks(full_text, query, summarizer,
                              min_para_len=250, max_chunks=10,
                              chunk_size=2000, overlap=300,
                              min_summary_len=100, max_summary_len=300,
                              max_chunk_chars=2000):
    """
    Extract relevant paragraphs, merge into large chunks,
    rank chunks independently (AND → OR), and summarize top chunks.
    Only truncate chunks if exceeding model max length to preserve flow.
    """

    # Step 1: Extract relevant paragraphs
    matched_paragraphs = extract_relevant_paragraphs(
        full_text, query, min_len=min_para_len, max_chunks=max_chunks, fallback_sentence_chunk=8
    )

    if not matched_paragraphs:
        print("No relevant paragraphs found. Using fallback.")
        return full_text[:1000]

    print(f"Found {len(matched_paragraphs)} relevant paragraphs.")

    # Step 2: Merge paragraphs into overlapping chunks
    text_to_summarize = " ".join(matched_paragraphs)
    n = len(text_to_summarize)
    chunks = []
    start = 0
    while start < n:
        end = start + chunk_size
        chunks.append(text_to_summarize[start:end])
        start += (chunk_size - overlap)

    # Step 3: Rank chunks independently (AND → OR)
    query_terms = [t.lower() for t in re.findall(r'\b\w+\b', query)]
    ranked_chunks = []

    for idx, chunk in enumerate(chunks):
        rank, rank_type = chunk_rank(chunk, query_terms)
        if rank > 0:
            ranked_chunks.append((chunk, rank, rank_type))
            print(f"Chunk {idx+1}: rank={rank} ({rank_type})")

    if not ranked_chunks:
        print("No chunks matched query terms. Using fallback.")
        ranked_chunks = [(text_to_summarize[:chunk_size], 0, "fallback")]

    # Sort by rank descending (AND first, then OR)
    ranked_chunks.sort(key=lambda x: x[1], reverse=True)
    top_chunks = [x[0] for x in ranked_chunks[:max_chunks]]

    print(f"Summarizing top {len(top_chunks)} ranked chunks...")

    # Step 4: Summarize top chunks safely
    summaries = []
    for idx, chunk in enumerate(top_chunks):
        # Only truncate if chunk is too long for the model
        if len(chunk) > max_chunk_chars:
            chunk_to_summarize = chunk[:max_chunk_chars]
            print(f"Chunk {idx+1} truncated to {len(chunk_to_summarize)} chars for model.")
        else:
            chunk_to_summarize = chunk

        try:
            result = summarizer(
                chunk_to_summarize,
                max_length=max_summary_len,
                min_length=min_summary_len,
                do_sample=False
            )
            if result and isinstance(result, list) and "summary_text" in result[0]:
                summaries.append(result[0]["summary_text"].strip())
                print(f"Chunk {idx+1} summary preview: {result[0]['summary_text'][:100]}...")
            else:
                print(f"⚠️ Chunk {idx+1} summarizer returned empty output. Skipping.")
        except Exception as e:
            print(f"⚠️ Skipping chunk {idx+1} due to summarization error: {e}")

    # Step 5: Merge summaries
    final_summary = " ".join(summaries)
    return final_summary.strip()


# Independent AND/OR ranking for a single chunk
def chunk_rank(chunk, query_terms):
    """
    Rank a single chunk based on query terms:
    - AND if all query terms present
    - OR if some terms present
    - 0 if none
    Does NOT aggregate across chunks.
    """
    chunk_tokens = set(re.findall(r'\b\w+\b', chunk.lower()))
    matched_terms = [t for t in query_terms if t in chunk_tokens]

    if not matched_terms:
        return 0, None
    elif len(matched_terms) == len(query_terms):
        return 2, "AND"   # highest priority
    else:
        return 1, "OR"    # partial match, lower priority

# 📊 --- Analysis helper ---
def analyze_text_summary(text):
    """Return top entities and key keywords from text summary."""
    analysis = {"entities": [], "keywords": []}
    if not text:
        return analysis

    try:
        entities = ner_analyzer(text)
        keywords = kw_model.extract_keywords(text, top_n=10)
        analysis["entities"] = [{"entity": e["entity_group"], "text": e["word"]} for e in entities]
        analysis["keywords"] = [kw[0] for kw in keywords]
    except Exception as e:
        print("Analysis error:", e)
    return analysis


import re

import re

import re
from itertools import combinations

import re
from itertools import combinations

def extract_relevant_paragraphs(full_text, query, min_len=250, max_chunks=10, fallback_sentence_chunk=8):
    """
    Extract paragraphs that match the query progressively:
    - Try paragraphs containing all query terms (AND)
    - Then paragraphs containing subsets of terms (OR)
    - Rank paragraphs by number of query terms present
    """
    query_terms = [t.lower() for t in re.findall(r'\b\w+\b', query)]
    # Split text into paragraphs using double newline or line breaks
    paragraphs = re.split(r'\n{2,}|\r{2,}|\n', full_text)
    ranked_paragraphs = []

    # Iterate progressively: try all terms → N-1 terms → ... → 1 term
    for num_terms in range(len(query_terms), 0, -1):
        for para in paragraphs:
            para_text = para.strip()
            if len(para_text) < min_len:
                continue
            para_tokens = set(re.findall(r'\b\w+\b', para_text.lower()))
            # Count how many query terms are present
            matched_terms = [t for t in query_terms if t in para_tokens]
            if len(matched_terms) >= num_terms:
                ranked_paragraphs.append((para_text, len(matched_terms)))
        if ranked_paragraphs:
            # Stop after finding paragraphs with current number of terms
            break

    # Sort paragraphs by matched terms descending
    ranked_paragraphs.sort(key=lambda x: x[1], reverse=True)
    selected_paragraphs = [p[0] for p in ranked_paragraphs[:max_chunks]]

    # Fallback: if no paragraph found, split text into sentence chunks
    if not selected_paragraphs:
        sentences = re.split(r'(?<=[.!?])\s+', full_text)
        chunks = [" ".join(sentences[i:i+fallback_sentence_chunk])
                  for i in range(0, len(sentences), fallback_sentence_chunk)]
        selected_paragraphs = chunks[:max_chunks]

    return selected_paragraphs

import re
import math
from itertools import combinations

def rank_and_score_document(full_text, query, query_terms, base_score=0.0):
    """
    Robust document ranking:
    - Progressive AND → OR term matching
    - Phrase + term boosts (exact word matches)
    - Returns final_score, rank_type
    """
    if not full_text or not query_terms:
        return base_score, "none"

    # --- Tokenize document ---
    tokens = re.findall(r'\b\w+\b', full_text.lower())
    token_set = set(tokens)

    # --- Progressive AND → OR matching ---
    n_terms = len(query_terms)
    rank_type = "none"
    matched_terms_count = 0
    for num_terms in range(n_terms, 0, -1):
        for combo in combinations(query_terms, num_terms):
            if all(term.lower() in token_set for term in combo):
                matched_terms_count = num_terms
                rank_type = "AND" if num_terms == n_terms else f"OR-{num_terms}"
                break
        if matched_terms_count:
            break

    # --- Phrase & term boosts ---
    # Count exact query terms
    term_count = sum(tokens.count(term.lower()) for term in query_terms)

    # Count exact query phrase (all terms together)
    phrase_pattern = r'\b' + r'\s+'.join(re.escape(term.lower()) for term in query_terms) + r'\b'
    phrase_count = len(re.findall(phrase_pattern, full_text.lower()))

    weight = (phrase_count * 2.0) + (term_count * 0.2) + matched_terms_count
    boost = min(2.0, math.log1p(weight))
    final_score = base_score + boost

    return final_score, rank_type


# 🧩 --- Your existing search route with summarization + analysis integrated ---
@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "").strip()
    department_filter = request.args.get("department", None)
    summarize_flag = request.args.get("summarize", "false").lower() == "true"
    analyze_flag = request.args.get("analyze", "false").lower() == "true"

    app.logger.info(f"Search query='{query}' summarize={summarize_flag} analyze={analyze_flag}")

    if not query:
        return render_template("search.html", results=[], query="") \
            if request.accept_mimetypes.accept_html else jsonify({"type": "none", "results": []})

    # --- Refine query terms ---
    refined_terms = refine_query_terms(query)
    query_terms = [t.lower() for t in refined_terms]
    query_lower = " ".join(query_terms)

    # --- Step 1: Text search ---
    results, result_type = run_search(query)
    if not isinstance(results, list):
        results = []
    all_docs = {str(r["_id"]): r for r in results}

    # --- Step 2: Semantic fallback ---
    if len(all_docs) < 5:
        sem_results = semantic_search(query, top_k=10)
        for doc in sem_results:
            fid = str(doc["_id"])
            if fid not in all_docs:
                all_docs[fid] = doc

    # --- Step 3: Department filter ---
    if department_filter:
        all_docs = {
            k: v for k, v in all_docs.items()
            if v.get("department", "").lower() == department_filter.lower()
        }

    # --- Step 4: Scoring, boosts, snippets ---
    clean_results = []
    for doc in all_docs.values():
        # Prepare full text
        full_text = doc.get("data") or doc.get("search_text") or ""
        if isinstance(full_text, list):
            full_text = " ".join(map(str, full_text))
        elif isinstance(full_text, dict):
            full_text = " ".join(f"{k}: {v}" for k, v in full_text.items())
        elif not isinstance(full_text, str):
            full_text = str(full_text)

        text_lower = full_text.lower()
        tokens = re.findall(r'\b\w+\b', text_lower)

        # Phrase and term counts
        phrase_pattern = r'\b' + r'\s+'.join(re.escape(term) for term in query_terms) + r'\b'
        phrase_count = len(re.findall(phrase_pattern, text_lower))
        term_count = sum(tokens.count(term) for term in query_terms)

        # Weight and boost
        weight = (phrase_count * 2.0) + (term_count * 0.2)
        boost = min(2.0, math.log1p(weight))
        base_score = float(doc.get("score") or doc.get("semantic_score") or 0.0)
        final_score = base_score + boost

        # Snippet generation
        snippet = make_snippet(full_text, query)
        if not snippet:
            snippet = full_text[:150] + "..." if len(full_text) > 150 else full_text

        # Save document info
        result_item = {
            "_id": str(doc["_id"]),
            "filename": doc.get("filename", "unknown"),
            "filetype": doc.get("filetype", "unknown"),
            "_snippet": snippet,
            "score": round(final_score, 3),
            "phrase_hits": phrase_count,
            "word_hits": term_count,
            "boost": round(boost, 3)
        }
        clean_results.append(result_item)

    # Sort by score descending
    clean_results.sort(key=lambda x: x["score"], reverse=True)

    # Filter out zero-hit docs if multiple exist
    if len(clean_results) > 1:
        filtered = [r for r in clean_results if r["phrase_hits"] > 0 or r["word_hits"] > 0]
        if filtered:
            clean_results = filtered

    # --- Step 5: Summarize top documents ---
    final_summary = None
    if summarize_flag and clean_results:
        combined_summaries = []
        for result_item in clean_results[:3]:  # top 3 docs
            doc = all_docs.get(result_item["_id"])
            if not doc:
                continue

            full_text = doc.get("data") or doc.get("search_text") or ""
            if isinstance(full_text, list):
                full_text = " ".join(map(str, full_text))
            elif isinstance(full_text, dict):
                full_text = " ".join(f"{k}: {v}" for k, v in full_text.items())
            elif not isinstance(full_text, str):
                full_text = str(full_text)

            try:
                focused_summary = summarize_relevant_chunks(
                    full_text,
                    query,
                    summarizer=summarizer,
                    min_para_len=250,
                    max_chunks=10,
                    chunk_size=2500,
                    overlap=200,
                    min_summary_len=100,
                    max_summary_len=300
                )
                if focused_summary:
                    combined_summaries.append(focused_summary)
            except Exception as e:
                app.logger.error(f"Failed to summarize document {result_item['_id']}: {e}", exc_info=True)

        if combined_summaries:
            final_summary = "\n\n".join(combined_summaries)

    # --- Step 6: Optional analysis ---
    file_text = " ".join([r.get("_snippet", "") for r in clean_results])
    analysis = analyze_text_summary(file_text) if analyze_flag else None

    # --- Step 7: Response ---
    response = {"type": "mixed", "results": clean_results}
    if final_summary:
        response["summary"] = final_summary
    if analysis:
        response["analysis"] = analysis

    if request.accept_mimetypes.accept_html:
        return render_template(
            "search.html",
            results=clean_results,
            query=query,
            type="mixed",
            summary=final_summary,
            analysis=analysis
        )

    return jsonify(response)



@app.route("/export_excel/<file_id>")
def export_excel(file_id):
    file_doc = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    if not file_doc or "data" not in file_doc:
        flash("File or data not found.", "danger")
        return redirect(url_for("dashboard"))
    excel_stream = write_excel_from_data(file_doc["filename"], file_doc["filetype"], file_doc["data"])
    return send_file(excel_stream, download_name=f"{file_doc['filename']}_data.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")



@app.route("/export_word/<file_id>")
def export_word(file_id):
    # Fetch file document from MongoDB
    file_doc = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    if not file_doc or "data" not in file_doc:
        flash("File or data not found.", "danger")
        return redirect(url_for("dashboard"))

    # Generate Word file stream
    word_stream = write_word_from_data(
        file_name_prefix=file_doc.get("filename", "exported_file"),
        filetype=file_doc.get("filetype", "unknown"),
        data=file_doc["data"]
    )

    # Send as downloadable file
    return send_file(
        word_stream,
        download_name=f"{file_doc.get('filename','exported_file')}_data.docx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )


@app.route('/file/<file_id>')
def file_detail(file_id):
    # Fetch file from DB by ObjectId
    try:
        file_doc = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    except Exception:
        flash("Invalid file ID.", "danger")
        return redirect(url_for("search"))

    if not file_doc:
        flash("File not found.", "danger")
        return redirect(url_for("search"))

    # Optional: pass search query for highlighting
    query = request.args.get("q", "")

    # Ensure all needed fields are prepared for template
    file_data = {
        "_id": str(file_doc["_id"]),
        "file_id": str(file_doc.get("file_id")),
        "filename": file_doc.get("filename", "unknown"),
        "filetype": file_doc.get("filetype", "unknown"),
        "data": file_doc.get("data", {}),
    }

    return render_template("view_file.html", file=file_data, query=query)


@app.route("/file/<file_id>/export/download")
def export_download_file(file_id):
    try:
        grid_out = fs.get(ObjectId(file_id))
        return send_file(
            io.BytesIO(grid_out.read()),
            download_name=grid_out.filename,
            mimetype=grid_out.content_type or "application/octet-stream",
            as_attachment=True
        )
    except NoFile:
        abort(404, description="File not found in storage.")


@app.route("/file/<file_id>/export/excel")
def export_file_excel(file_id):
    file_doc = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    if not file_doc or "data" not in file_doc:
        flash("File or data not found.", "danger")
        return redirect(url_for("dashboard"))
    excel_stream = write_excel_from_data(file_doc["filename"], file_doc["filetype"], file_doc["data"])
    return send_file(excel_stream, download_name=f"{file_doc['filename']}_data.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/file/<file_id>/export/word")
def export_file_word(file_id):
    # Fetch file document from MongoDB
    file_doc = mongo.db.files.find_one({"_id": ObjectId(file_id)})
    if not file_doc or "data" not in file_doc:
        flash("File or data not found.", "danger")
        return redirect(url_for("dashboard"))

    # Generate Word file stream
    word_stream = write_word_from_data(
        file_name_prefix=file_doc.get("filename", "exported_file"),
        filetype=file_doc.get("filetype", "unknown"),
        data=file_doc["data"]
    )

    # Send as downloadable file
    return send_file(
        word_stream,
        download_name=f"{file_doc.get('filename','exported_file')}_data.docx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
@app.errorhandler(413)
def request_entity_too_large(error):
    flash("File size exceeds maximum allowed size.", "danger")
    return redirect(request.url)


@app.errorhandler(400)
def bad_request(e):
    return render_template("400.html", error=e), 400


@app.errorhandler(500)
def internal_error(e):
    return render_template("500.html", error=e), 500

# --------------------------- Admin endpoint to rebuild FAISS --------------------
# Protect with ADMIN_KEY env var in production

ADMIN_KEY = os.getenv("ADMIN_KEY")

@app.route("/admin/rebuild_faiss", methods=["POST"])
def admin_rebuild_faiss():
    key = request.args.get("key") or request.headers.get("X-ADMIN-KEY")
    if ADMIN_KEY and key != ADMIN_KEY:
        return "unauthorized", 401
    try:
        total = rebuild_faiss()
        return jsonify({"status": "ok", "vectors": total})
    except Exception as e:
        app.logger.exception("Rebuild failed: %s", e)
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    serve(app, host="10.206.41.36", port=5006)