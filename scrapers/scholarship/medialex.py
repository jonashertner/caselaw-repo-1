"""medialex.ch (Zs. für Medienrecht) — WordPress REST API harvester."""
from .wordpress_rest import MedialexAdapter

def harvest(*, max_records=None, **kwargs):
    return MedialexAdapter.harvest(max_records=max_records, **kwargs)
