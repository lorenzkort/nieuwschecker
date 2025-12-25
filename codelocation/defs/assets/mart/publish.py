import dagster as dg

@dg.asset(
    ins={
        "cross_feed_clusters": dg.AssetIn(["staging", "cross_feed_clusters"])
    }
)
def publish_sentence():
    from definitions import defs
    df = defs.load_asset_value(["staging", "cross_feed_clusters"])
    df
    return