import os
from datetime import datetime

import dagster as dg
import polars as pl
from polars import DataFrame
from utils.ftp_manager import StratoUploader
from utils.utils import DATA_DIR

logging = dg.get_dagster_logger()


@dg.asset(
    key_prefix="mart",
    ins={
        "timeline": dg.AssetIn(["staging", "timeline"]),
        "agency_owners": dg.AssetIn(["raw", "agency_owners"]),
    },
)
def create_publish_timeline_html(timeline: DataFrame, agency_owners: DataFrame) -> None:
    cloudflare_site_token = os.environ.get("CLOUDFLARE_SITE_TOKEN")

    cluster_publish_delay_hours = 5
    timeline_cutoff_days = 14

    output_path = DATA_DIR / "website" / "index.html"
    server_path = "nieuwschecker/"

    now = datetime.now()

    df = (
        timeline.filter(  # show relevant clusters only
            (pl.col("num_feeds") > 8)
            # | (pl.col("blindspot_left") == 1)
            # | (pl.col("blindspot_right") == 1)
            # | (pl.col("single_owner_high_reach") == 1)
        )
        .filter(  # only show clusters older than delay
            (
                pl.col("max_published_date")
                < (now - pl.duration(hours=cluster_publish_delay_hours))
            )
            & (
                pl.col("max_published_date")
                > (now - pl.duration(days=timeline_cutoff_days))
            )
        )
        .sort("max_published_date", descending=True)
    )

    #!/usr/bin/env python3
    """
    Ground News Style Timeline POC
    Displays news articles in a mobile-friendly timeline format
    """

    logging.info(f"Generating timeline with {len(df)} news clusters...")

    # HTML Template
    html_template = """<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Nieuws Checker (beta)</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background-color: #f5f5f5;
                padding: 12px;
                line-height: 1.4;
                max-width: 430px;
                margin: 0 auto;
            }}
            
            .container {{
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                overflow: hidden;
                margin-bottom: 12px;
                cursor: pointer;
                transition: transform 0.2s, box-shadow 0.2s;
            }}
            
            .container:active {{
                transform: scale(0.98);
            }}
            
            .header {{
                padding: 14px;
                border-bottom: 1px solid #e5e5e5;
            }}
            
            .cluster-title {{
                font-size: 16px;
                font-weight: 600;
                color: #1a1a1a;
                margin-bottom: 8px;
                line-height: 1.3;
            }}
            
            .metadata {{
                display: flex;
                gap: 12px;
                font-size: 12px;
                color: #666;
            }}
            
            .metadata-item {{
                display: flex;
                align-items: center;
                gap: 4px;
            }}
            
            .bias-section {{
                padding: 12px 14px;
            }}
            
            .bias-bar-container {{
                height: 24px;
                background-color: #f0f0f0;
                border-radius: 4px;
                overflow: hidden;
                display: flex;
            }}
            
            .bias-segment {{
                height: 100%;
                transition: all 0.3s ease;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 10px;
                font-weight: 600;
                color: white;
                text-shadow: 0 1px 2px rgba(0,0,0,0.2);
            }}
            
            .bias-left {{
                background-color: #4A90E2;
            }}
            
            .bias-centre-left {{
                background-color: #7FB3E8;
            }}
            
            .bias-centre {{
                background-color: #9CA3AF;
            }}
            
            .bias-centre-right {{
                background-color: #E89B7F;
            }}
            
            .bias-right {{
                background-color: #E24A4A;
            }}
        
            .articles-section {{
                max-height: 0;
                overflow: hidden;
                transition: max-height 0.3s ease-out;
            }}
            
            .articles-section.expanded {{
                max-height: 2000px;
                transition: max-height 0.5s ease-in;
            }}
            
            .articles-inner {{
                padding: 14px;
                border-top: 1px solid #e5e5e5;
            }}
            
            .section-title {{
                font-size: 11px;
                font-weight: 600;
                color: #666;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 10px;
            }}
            
            .article-item {{
                padding: 8px 0;
                border-bottom: 1px solid #f0f0f0;
            }}
            
            .article-item:last-child {{
                border-bottom: none;
            }}
            
            .article-link {{
                color: #4A90E2;
                text-decoration: none;
                font-size: 13px;
                font-weight: 500;
                display: block;
                margin-bottom: 3px;
                line-height: 1.3;
            }}
            
            .article-link:hover {{
                text-decoration: underline;
            }}
            
            .article-meta {{
                font-size: 11px;
                color: #999;
            }}
            
            .reach-info {{
                font-size: 11px;
                color: #666;
                padding: 8px 14px;
                background-color: #f9f9f9;
                border-top: 1px solid #e5e5e5;
            }}
            
            .expand-indicator {{
                text-align: center;
                padding: 8px;
                font-size: 11px;
                color: #4A90E2;
                border-top: 1px solid #e5e5e5;
            }}
            
            .expand-indicator::after {{
                content: '▼';
                margin-left: 4px;
                font-size: 9px;
            }}
            
            .container.expanded .expand-indicator::after {{
                content: '▲';
            }}
        </style>
    </head>
    <body>
    <div><span>Nieuws Checker (beta) - lorenz.kort@gmail.com<br></br></span></div>
    {news_clusters}
    <!-- Cloudflare Web Analytics --><script defer src='https://static.cloudflareinsights.com/beacon.min.js' data-cf-beacon='{{"token": "{cloudflare_site_token}" }}'></script><!-- End Cloudflare Web Analytics -->
    <script>
        // Add click handlers to all containers
        document.querySelectorAll('.container').forEach(container => {{
            container.addEventListener('click', (e) => {{
                // Prevent expansion if clicking on a link
                if (e.target.tagName === 'A') return;
                
                e.preventDefault();
                e.stopPropagation();
                
                // Toggle expanded state
                container.classList.toggle('expanded');
                const articlesSection = container.querySelector('.articles-section');
                articlesSection.classList.toggle('expanded');
            }});
            
            // Prevent link clicks from toggling
            container.querySelectorAll('a').forEach(link => {{
                link.addEventListener('click', (e) => {{
                    e.stopPropagation();
                }});
            }});
        }});
    </script>
    </body>
    </html>
    """

    # Generate news clusters HTML
    clusters_html = []

    for row in df.iter_rows(named=True):
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

            label = f"{label_text} {perc}%" if perc >= 1 else ""
            bias_segments.append(
                f'<div class="bias-segment {css_class}" style="width: {perc}%">{label}</div>'
            )

        bias_bar_html = "".join(bias_segments)

        # Generate ownership distribution - stacked bar like bias chart
        owners = row.get("owner_reach") or []
        owners_sorted = sorted(
            owners, key=lambda o: o.get("total_reach", 0), reverse=True
        )
        total_reach = sum(o.get("total_reach", 0) for o in owners)

        owner_segments = []

        for o in owners_sorted:
            reach_percentage = (o.get("total_reach") / total_reach) * 100

            # Only show owners with more than 15%
            if reach_percentage <= 10:
                continue

            owner = o.get("owner")
            color = agency_owners.filter(pl.col("owner") == owner)["color"].item(0)

            # Show label with percentage, similar to bias chart
            perc_int = int(reach_percentage)
            label = f"{owner} {perc_int}%" if perc_int >= 1 else ""

            owner_segments.append(
                f'<div  class="bias-segment" style="width: {reach_percentage}%; background-color: {color}">{label}</div>'
            )

        owner_bar_html = "".join(owner_segments)

        # Generate articles HTML
        articles_html_items = []
        for article in row["articles"]:
            time_str = article["publish_date"]
            article_html = f"""
                    <div class="article-item">
                        <a href="{article["link"]}" class="article-link">{article["title"]}</a>
                        <div class="article-meta">{article["feed"]} • {time_str}</div>
                    </div>"""
            articles_html_items.append(article_html)

        articles_html = "".join(articles_html_items)

        # Build cluster HTML using condensed template
        cluster_html = f"""
        <div class="container">
            <div class="header">
                <h1 class="cluster-title">{row["title"]}</h1>
                <div class="metadata">
                    <div class="metadata-item">
                        <span>{row["num_articles"]} nieuwsberichten</span>
                    </div>
                    <div class="metadata-item">
                        <span>{row["num_feeds"]} bronnen</span>
                    </div>
                    <div class="metadata-item">
                        <span>{row["max_published_date_fmt"]}</span>
                    </div>
                </div>
            </div>
            
            <div class="bias-section">
                <div class="bias-bar-container">
                    {bias_bar_html}
                </div>
                <br>
                <div class="bias-bar-container">
                    {owner_bar_html}
                </div>            
            </div>            
            
            <div class="expand-indicator">
                Bekijk nieuwsberichten
            </div>
            
            <div class="articles-section">
                <div class="articles-inner">
                    <h2 class="section-title">Nieuwsberichten ({row["num_articles"]})</h2>
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
