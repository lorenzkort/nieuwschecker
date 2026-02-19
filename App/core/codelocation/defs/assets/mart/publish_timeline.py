import os

import dagster as dg
import polars as pl
from polars import DataFrame
from utils.ftp_manager import StratoUploader
from utils.utils import DATA_DIR

logging = dg.get_dagster_logger()


def _df_to_html_table(df: pl.DataFrame) -> str:
    headers = "".join(f"<th>{c}</th>" for c in df.columns)

    rows = ""
    for row in df.iter_rows():
        cells = "".join(f"<td>{cell}</td>" for cell in row)
        rows += f"<tr>{cells}</tr>"

    return f"""
        <table class="popup-table">
            <thead>
                <tr>{headers}</tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    """


def _reach_bar_color(left_right: float) -> str:
    if left_right <= -0.3:
        return "#2563EB"  # left
    elif left_right <= 0.15:
        return "#6B7280"  # centre
    elif left_right <= 0.5:
        return "#B06048"  # centre-right
    else:
        return "#DC2626"  # right


def _format_reach(reach: int) -> str:
    if reach >= 1_000_000:
        return f"{reach / 1_000_000:.1f}M"
    elif reach >= 1_000:
        return f"{reach / 1_000:.0f}K"
    return str(reach)


def _df_to_reach_bars(df: pl.DataFrame) -> str:
    """Generate a distribution strip chart: agencies plotted on a left–right axis, bar height = reach."""
    if df.is_empty():
        return "<p class='dist-empty'>Geen data beschikbaar</p>"

    max_reach_raw = df["Bereik"].max()
    max_reach: float = float(max_reach_raw) if max_reach_raw else 1.0

    sorted_df = df.sort("Links (-) Rechts (+)")
    rows = []
    for row in sorted_df.iter_rows(named=True):
        reach = int(row["Bereik"] or 0)
        lr = float(row["Links (-) Rechts (+)"] or 0)
        # Map lr from [-1, 1] to [0%, 100%] position on spectrum
        pos_pct = ((lr + 1) / 2) * 100
        # Bar width proportional to reach (min 4%, max 60%)
        bar_w = max(4, min(60, int((reach / max_reach) * 60)))
        color = _reach_bar_color(lr)
        rows.append(
            f"""
            <div class="dist-row">
                <div class="dist-label">{row["Medium"]}</div>
                <div class="dist-track">
                    <div class="dist-marker" style="left:{pos_pct:.1f}%;width:{bar_w:.0f}px;background:{color};" title="{row['Medium']} — {_format_reach(reach)}"></div>
                </div>
                <div class="dist-reach">{_format_reach(reach)}</div>
            </div>"""
        )
    return "".join(rows)


@dg.asset(
    key_prefix="mart",
    ins={
        "timeline": dg.AssetIn(["staging", "timeline"]),
        "agency_owners": dg.AssetIn(["raw", "agency_owners"]),
        "news_agencies": dg.AssetIn(["raw", "news_agencies"]),
    },
)
def create_publish_timeline_html(
    timeline: DataFrame, agency_owners: DataFrame, news_agencies: DataFrame
) -> None:
    cloudflare_site_token = os.environ.get("CLOUDFLARE_SITE_TOKEN")

    output_path = DATA_DIR / "website" / "index.html"
    server_path = "nieuwschecker/"

    df = timeline

    media_lookup = (
        news_agencies.filter(pl.col("rss_available") == 1)
        .with_columns(
            pl.col("url").alias("Medium"),
            pl.col("reach").alias("Bereik"),
            pl.col("left_right").alias("Links (-) Rechts (+)"),
        )
        .select(["Medium", "Bereik", "Links (-) Rechts (+)"])
    )

    """
    Ground News Style Timeline POC
    Displays news articles in a mobile-friendly timeline format
    """

    logging.info(f"Generating timeline with {len(df)} news clusters...")

    # Debug: log blindspot stats
    if "blindspot_left" in df.columns and "blindspot_right" in df.columns:
        n_left = df.filter(df["blindspot_left"] == 1).height
        n_right = df.filter(df["blindspot_right"] == 1).height
        logging.info(f"Blindspot clusters — left: {n_left}, right: {n_right}")
        if n_left > 0:
            logging.info(
                f"  blindspot_left titles: {df.filter(df['blindspot_left'] == 1)['title'].to_list()}"
            )
        if n_right > 0:
            logging.info(
                f"  blindspot_right titles: {df.filter(df['blindspot_right'] == 1)['title'].to_list()}"
            )
    else:
        logging.warning("blindspot_left / blindspot_right columns NOT found in df!")

    # HTML Template
    html_template = """<!DOCTYPE html>
    <html lang="nl">
    <head>
        <link rel="icon" type="image/svg+xml" href="/favicon.svg">
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Nieuws Checker</title>
        <meta name="description" content="Nieuws Checker — onafhankelijk overzicht van het Nederlandse medialandschap">
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {{
                --color-bg-primary:        #F5F0E8;
                --color-bg-secondary:      #EDEDE5;
                --color-bg-inverse:        #111111;
                --color-text-primary:      #1A1A1A;
                --color-text-secondary:    #5A5A5A;
                --color-text-inverse:      #F5F0E8;
                --color-border:            #D0C9BA;
                --color-border-strong:     #8A8070;
                --color-accent:            #C8391A;

                --color-bias-left:         #2563EB;
                --color-bias-center:       #6B7280;
                --color-bias-right:        #DC2626;
                --color-bias-center-left:  #4B82D4;
                --color-bias-center-right: #B55A3A;

                --font-body: 'Inter', system-ui, -apple-system, sans-serif;
                --font-mono: 'JetBrains Mono', 'Fira Code', monospace;
            }}

            *, *::before, *::after {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}

            body {{
                font-family: var(--font-body);
                background-color: var(--color-bg-primary);
                color: var(--color-text-primary);
                line-height: 1.6;
                font-size: 16px;
                -webkit-font-smoothing: antialiased;
                -moz-osx-font-smoothing: grayscale;
            }}

            .site-header {{
                position: sticky;
                top: 0;
                z-index: 100;
                height: 56px;
                background: var(--color-bg-primary);
                border-bottom: 1px solid var(--color-border-strong);
                display: flex;
                align-items: center;
                padding: 0 24px;
            }}

            .site-header h1 {{
                font-family: var(--font-body);
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--color-text-primary);
            }}

            .site-header .header-meta {{
                margin-left: auto;
                font-size: 12px;
                font-weight: 500;
                color: var(--color-text-secondary);
                letter-spacing: 0.04em;
            }}

            .site-header .header-meta a {{
                color: var(--color-text-secondary);
                text-decoration: none;
            }}

            .site-header .header-meta a:hover {{
                color: var(--color-text-primary);
            }}

            .content-area {{
                max-width: 720px;
                margin: 0 auto;
                padding: 16px 24px;
            }}

            /* ── Cluster card ── */
            .container {{
                background: var(--color-bg-primary);
                border: 1px solid var(--color-border);
                border-radius: 0;
                overflow: hidden;
                margin-bottom: -1px;
            }}

            .header {{
                padding: 14px 16px 12px;
                border-bottom: 1px solid var(--color-border);
            }}

            .cluster-title {{
                font-size: 20px;
                font-weight: 700;
                color: var(--color-text-primary);
                letter-spacing: -0.02em;
                margin-bottom: 6px;
                line-height: 1.3;
                flex: 1;
            }}

            .title-row {{
                display: flex;
                align-items: flex-start;
                gap: 12px;
                margin-bottom: 6px;
            }}

            .metadata {{
                display: flex;
                gap: 16px;
            }}

            .metadata-item {{
                display: flex;
                align-items: center;
                gap: 4px;
                font-size: 12px;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: var(--color-text-secondary);
            }}

            .metadata-item .sep {{
                margin: 0 2px;
                color: var(--color-border-strong);
            }}

            /* ── Blindspot tag ── */
            .blindspot-tag {{
                display: inline-block;
                flex-shrink: 0;
                align-self: center;
                font-size: 11px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                padding: 4px 10px;
                border-radius: 4px;
                white-space: nowrap;
            }}

            .blindspot-tag--left {{
                background: #1D4ED8;
                color: #FFFFFF;
            }}

            .blindspot-tag--right {{
                background: #B91C1C;
                color: #FFFFFF;
            }}

            .tag-short {{
                display: none;
            }}

            @media (max-width: 480px) {{
                .tag-full {{
                    display: none;
                }}
                .tag-short {{
                    display: inline;
                }}
                .blindspot-tag {{
                    white-space: normal;
                    text-align: center;
                    line-height: 1.3;
                }}
            }}

            /* ── Bias bar ── */
            .bias-section {{
                padding: 10px 16px;
                border-bottom: 1px solid var(--color-border);
            }}

            .bias-label {{
                font-size: 12px;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: var(--color-text-secondary);
                margin-bottom: 6px;
            }}

            .bias-bar-container {{
                height: 14px;
                background-color: var(--color-bg-secondary);
                border-radius: 0;
                overflow: hidden;
                display: flex;
                cursor: pointer;
            }}

            .bias-segment {{
                height: 100%;
            }}

            .bias-left {{
                background-color: var(--color-bias-left);
            }}

            .bias-centre-left {{
                background-color: var(--color-bias-center-left);
            }}

            .bias-centre {{
                background-color: var(--color-bias-center);
            }}

            .bias-centre-right {{
                background-color: var(--color-bias-center-right);
            }}

            .bias-right {{
                background-color: var(--color-bias-right);
            }}

            .bias-legend {{
                display: flex;
                justify-content: space-between;
                margin-top: 4px;
                font-size: 12px;
                font-weight: 500;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--color-text-secondary);
            }}

            /* ── Expand / collapse ── */
            .expand-indicator {{
                text-align: center;
                padding: 8px 16px;
                font-size: 12px;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: var(--color-text-secondary);
                border-top: 1px solid var(--color-border);
                cursor: pointer;
                user-select: none;
                transition: color 0.15s;
            }}

            .expand-indicator:hover {{
                color: var(--color-text-primary);
            }}

            .expand-indicator::after {{
                content: ' ↓';
                font-size: 12px;
            }}

            .container.expanded .expand-indicator::after {{
                content: ' ↑';
            }}

            /* ── Articles section ── */
            .articles-section {{
                max-height: 0;
                overflow: hidden;
                transition: max-height 0.3s ease-out;
            }}

            .articles-section.expanded {{
                max-height: 3000px;
                transition: max-height 0.5s ease-in;
            }}

            .articles-inner {{
                padding: 12px 16px;
                border-top: 1px solid var(--color-border);
            }}

            .section-title {{
                font-size: 12px;
                font-weight: 500;
                color: var(--color-text-secondary);
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 8px;
            }}

            .article-item {{
                padding: 10px 0;
                border-bottom: 1px solid var(--color-border);
            }}

            .article-item:last-child {{
                border-bottom: none;
            }}

            .article-link {{
                color: var(--color-text-primary);
                text-decoration: none;
                font-size: 14px;
                font-weight: 500;
                display: block;
                margin-bottom: 2px;
                line-height: 1.4;
            }}

            .article-link:hover {{
                text-decoration: underline;
                text-decoration-color: var(--color-border-strong);
                text-underline-offset: 2px;
            }}

            .article-meta {{
                font-size: 12px;
                font-weight: 500;
                color: var(--color-text-secondary);
                letter-spacing: 0.04em;
            }}

            /* ── Popup / reach chart ── */
            .media-popup {{
                display: none;
                position: fixed;
                inset: 0;
                background: rgba(17, 17, 17, 0.6);
                justify-content: center;
                align-items: center;
                z-index: 1000;
            }}

            .media-popup-content {{
                background: var(--color-bg-primary);
                border: 1px solid var(--color-border-strong);
                border-radius: 0;
                padding: 20px;
                max-height: 80vh;
                overflow-y: auto;
                width: calc(100vw - 32px);
                max-width: 500px;
            }}

            .popup-title {{
                font-size: 12px;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: var(--color-text-secondary);
                margin-bottom: 12px;
            }}

            .close-btn {{
                float: right;
                font-size: 20px;
                cursor: pointer;
                color: var(--color-text-secondary);
                line-height: 1;
            }}

            .close-btn:hover {{
                color: var(--color-text-primary);
            }}

            /* ── Distribution chart ── */
            .dist-row {{
                display: flex;
                align-items: center;
                gap: 8px;
                margin-bottom: 2px;
                padding: 3px 0;
                border-bottom: 1px solid var(--color-border);
            }}

            .dist-row:last-child {{
                border-bottom: none;
            }}

            .dist-label {{
                font-size: 12px;
                font-weight: 500;
                color: var(--color-text-primary);
                min-width: 110px;
                flex-shrink: 0;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}

            .dist-track {{
                flex: 1;
                height: 10px;
                position: relative;
                background: var(--color-bg-secondary);
                border-radius: 0;
            }}

            .dist-marker {{
                position: absolute;
                top: 0;
                height: 100%;
                border-radius: 0;
                transform: translateX(-50%);
            }}

            .dist-reach {{
                font-family: var(--font-mono);
                font-size: 12px;
                color: var(--color-text-secondary);
                min-width: 40px;
                text-align: right;
                flex-shrink: 0;
            }}

            .dist-empty {{
                font-size: 12px;
                color: var(--color-text-secondary);
                padding: 8px 0;
            }}

            .dist-axis {{
                display: flex;
                justify-content: space-between;
                font-size: 12px;
                font-weight: 500;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--color-text-secondary);
                padding: 4px 0 0;
                margin-left: 118px;
                margin-right: 48px;
            }}

            /* ── Footer ── */
            .site-footer {{
                max-width: 720px;
                margin: 24px auto 0;
                padding: 12px 24px;
                border-top: 1px solid var(--color-border-strong);
                font-size: 12px;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: var(--color-text-secondary);
                text-align: center;
            }}

            .site-footer a {{
                color: var(--color-text-secondary);
                text-decoration: none;
            }}

            .site-footer a:hover {{
                color: var(--color-text-primary);
            }}

            /* ── Mobile ── */
            @media (max-width: 480px) {{
                .content-area {{
                    padding: 12px 12px;
                }}
                .site-header {{
                    padding: 0 12px;
                }}
                .metadata {{
                    flex-wrap: wrap;
                    gap: 8px;
                }}
                .cluster-title {{
                    font-size: 16px;
                }}
            }}
        </style>
    </head>
    <body>
    <header class="site-header">
        <h1>Nieuws Checker</h1>
        <span class="header-meta"><a href="https://github.com/lorenzkort/nieuwschecker" target="_blank" rel="noopener">github</a></span>
    </header>
    <div class="content-area">
    {news_clusters}
    </div>
    <footer class="site-footer">Nieuws Checker &middot; Beta &middot; <a href="https://github.com/lorenzkort/nieuwschecker" target="_blank" rel="noopener">broncode</a></footer>
    <!-- Cloudflare Web Analytics --><script defer src='https://static.cloudflareinsights.com/beacon.min.js' data-cf-beacon='{{"token": "{cloudflare_site_token}" }}'></script><!-- End Cloudflare Web Analytics -->
    <script>
        function toggleArticles(element) {{
            const container = element.closest(".container");
            const articlesSection = container.querySelector(".articles-section");
            container.classList.toggle("expanded");
            articlesSection.classList.toggle("expanded");
        }}

        function openPopup(event, element) {{
            event.stopPropagation();
            element.closest(".bias-section").querySelector(".media-popup").style.display = "flex";
        }}

        document.addEventListener("click", function(event) {{
            if (event.target.classList.contains("media-popup")) {{
                event.target.style.display = "none";
                event.stopPropagation();
            }}
        }});
    </script>
    </body>
    </html>
    """

    # Generate news clusters HTML
    clusters_html = []

    # Interleave blindspot_left and blindspot_right clusters, then append normal ones
    left_rows = [r for r in df.iter_rows(named=True) if r.get("blindspot_left") == 1]
    right_rows = [r for r in df.iter_rows(named=True) if r.get("blindspot_right") == 1]
    normal_rows = [
        r
        for r in df.iter_rows(named=True)
        if r.get("blindspot_left") != 1 and r.get("blindspot_right") != 1
    ]

    ordered_rows = []
    li, ri = 0, 0
    toggle = "left"
    while li < len(left_rows) or ri < len(right_rows):
        if toggle == "left" and li < len(left_rows):
            ordered_rows.append(left_rows[li])
            li += 1
            toggle = "right"
        elif toggle == "right" and ri < len(right_rows):
            ordered_rows.append(right_rows[ri])
            ri += 1
            toggle = "left"
        elif li < len(left_rows):
            ordered_rows.append(left_rows[li])
            li += 1
        else:
            ordered_rows.append(right_rows[ri])
            ri += 1
    ordered_rows.extend(normal_rows)

    for row in ordered_rows:
        # Calculate bias percentages
        left_perc = int(row.get("left", 0) * 100) if "left" in row else 0
        centre_left_perc = int(row["centre left"] * 100)
        centre_perc = int(row["centre"] * 100)
        centre_right_perc = int(row["centre right"] * 100)
        right_perc = int(row["right"] * 100)

        bias_segments = []

        segments = [
            ("Links", "bias-left", left_perc),
            ("C-Links", "bias-centre-left", centre_left_perc),
            ("Centrum", "bias-centre", centre_perc),
            ("C-Rechts", "bias-centre-right", centre_right_perc),
            ("Rechts", "bias-right", right_perc),
        ]

        for label_text, css_class, perc in segments:
            if perc <= 0:
                continue

            bias_segments.append(
                f'<div class="bias-segment {css_class}" style="width: {perc}%"></div>'
            )

        bias_bar_html = "".join(bias_segments)

        # Generate articles HTML
        articles_html_items = []
        for article in row["articles"]:
            time_str = article["publish_date"]
            article_html = f"""
                    <div class="article-item">
                        <a href="{article["link"]}" class="article-link" target="_blank" rel="noopener">{article["title"]}</a>
                        <div class="article-meta">{article["feed"]} · {time_str}</div>
                    </div>"""
            articles_html_items.append(article_html)

        articles_html = "".join(articles_html_items)

        # Generate per-cluster media reach visualization
        cluster_feeds = [f for f in row["feeds"]]
        cluster_media = media_lookup.filter(pl.col("Medium").is_in(cluster_feeds))
        media_popup_html = (
            f'<div class="popup-title">Verdeling bronnen — links / rechts &amp; bereik</div>'
            + _df_to_reach_bars(cluster_media)
            + '<div class="dist-axis"><span>Links</span><span>Rechts</span></div>'
        )

        # Build blindspot tag if applicable
        if row.get("blindspot_left") == 1:
            blindspot_tag_html = '<span class="blindspot-tag blindspot-tag--left"><span class="tag-full">Blinde vlek voor links</span><span class="tag-short">Blinde vlek<br>voor links</span></span>'
        elif row.get("blindspot_right") == 1:
            blindspot_tag_html = '<span class="blindspot-tag blindspot-tag--right"><span class="tag-full">Blinde vlek voor rechts</span><span class="tag-short">Blinde vlek<br>voor rechts</span></span>'
        else:
            blindspot_tag_html = ""

        # Build cluster HTML using condensed template
        cluster_html = f"""
        <div class="container">
            <div class="header">
                <div class="title-row">
                    <h2 class="cluster-title">{row["title"]}</h2>
                    {blindspot_tag_html}
                </div>
                <div class="metadata">
                    <span class="metadata-item">{row["num_articles"]} berichten</span>
                    <span class="metadata-item">{row["num_feeds"]} bronnen</span>
                    <span class="metadata-item">{row["max_published_date_fmt"]}</span>
                </div>
            </div>
            <div class="bias-section">
                <div class="bias-bar-container" onclick="openPopup(event, this)">
                    {bias_bar_html}
                </div>
                <div class="bias-legend"><span>Links</span><span>Rechts</span></div>
                <div class="media-popup">
                    <div class="media-popup-content" onclick="event.stopPropagation()">
                        <span class="close-btn" onclick="this.closest('.media-popup').style.display='none'">&times;</span>
                        {media_popup_html}
                    </div>
                </div>
            </div>
            <div class="expand-indicator" onclick="toggleArticles(this)">
                Bekijk berichten
            </div>
            <div class="articles-section">
                <div class="articles-inner">
                    <h3 class="section-title">Nieuwsberichten ({row["num_articles"]})</h3>
                    {articles_html}
                </div>
            </div>
        </div>"""

        clusters_html.append(cluster_html)

    # Generate final HTML
    final_html = html_template.format(
        news_clusters="".join(clusters_html),
        cloudflare_site_token=cloudflare_site_token,
    )
    # Write to file
    logging.info(f"Writing timeline HTML to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_html)

    logging.info(f"Uploading to server at {server_path}...")
    StratoUploader().upload_file(
        local_file=output_path, remote_path=server_path, overwrite=True
    )
