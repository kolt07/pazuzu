# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding="utf-8")
from config.settings import Settings
from data.database.connection import MongoDBConnection

st = Settings()
MongoDBConnection.initialize(st)
db = MongoDBConnection.get_database()

print("llm_cache", db.llm_cache.estimated_document_count())
print("olx total", db.olx_listings.estimated_document_count())
print("olx detail.llm exists", db.olx_listings.count_documents({"detail.llm": {"$exists": True}}))
print("olx llm.addresses.0", db.olx_listings.count_documents({"detail.llm.addresses.0": {"$exists": True}}))
print("pz total", db.prozorro_auctions.estimated_document_count())
print("pz description_hash", db.prozorro_auctions.count_documents({"description_hash": {"$exists": True, "$ne": None}}))
print("pz address_refs", db.prozorro_auctions.count_documents({"auction_data.address_refs.0": {"$exists": True}}))
print("cache addresses.0", db.llm_cache.count_documents({"result.addresses.0": {"$exists": True}}))
print("unified total", db.unified_listings.estimated_document_count())
print("unified with addresses", db.unified_listings.count_documents({"addresses.0": {"$exists": True}}))

# kyievo samples
q = {"detail.llm.addresses": {"$elemMatch": {"$or": [
    {"district": {"$regex": "києво", "$options": "i"}},
    {"settlement_district": {"$regex": "києво", "$options": "i"}},
]}}}
print("olx kyievo in llm addresses", db.olx_listings.count_documents(q))

# where are pz addresses stored?
pz = db.prozorro_auctions.find_one({"description_hash": {"$exists": True, "$ne": None}})
if pz:
    print("pz sample has description_hash", pz.get("description_hash"))
    ad = pz.get("auction_data") or {}
    for k in ("address_refs", "parsed_info", "addresses", "llm"):
        print("  auction_data."+k, k in ad)
# check top-level fields beyond auction_data
pz2 = db.prozorro_auctions.find_one()
extra = [k for k in (pz2 or {}) if k not in ("_id","auction_id","auction_data","version_hash","description_hash","last_updated","created_at")]
print("pz extra keys", extra)

# unified sample addresses
u = db.unified_listings.find_one({"addresses.0": {"$exists": True}})
if u:
    print("unified source", u.get("source"), "region", u.get("region"))
    print("unified addresses[0]", (u.get("addresses") or [None])[0])
