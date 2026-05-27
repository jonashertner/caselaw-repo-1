"""eizpublishing.ch (EuZ — Zs. für Europarecht) — WordPress custom-type harvester."""
from .wordpress_rest import EizpublishingAdapter

def harvest(*, max_records=None, **kwargs):
    return EizpublishingAdapter.harvest(max_records=max_records, **kwargs)
