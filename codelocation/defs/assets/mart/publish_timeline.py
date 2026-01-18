import dagster as dg
from polars import DataFrame

logging = dg.get_dagster_logger()


@dg.asset(
    key_prefix="mart",
    ins={
        "timeline": dg.AssetIn(["staging", "timeline"]),
    },
)
def create_publish_timeline_html(timeline: DataFrame) -> None:
    from datetime import datetime

    import polars as pl
    from utils.ftp_manager import StratoUploader
    from utils.utils import DATA_DIR

    output_path = DATA_DIR / "website" / "index.html"
    server_path = "nieuwschecker/"

    df = timeline.filter(pl.col("num_feeds") > 8).sort(
        "max_published_date", descending=True
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
        <title>News Timeline</title>
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
    {news_clusters}

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

        # Build bias segments with labels
        bias_segments = []

        if left_perc > 0:
            label = f"Links {left_perc}%" if left_perc >= 8 else ""
            bias_segments.append(
                f'<div class="bias-segment bias-left" style="width: {left_perc}%">{label}</div>'
            )

        if centre_left_perc > 0:
            label = f"C-Links {centre_left_perc}%" if centre_left_perc >= 8 else ""
            bias_segments.append(
                f'<div class="bias-segment bias-centre-left" style="width: {centre_left_perc}%">{label}</div>'
            )

        if centre_perc > 0:
            label = f"Centrum {centre_perc}%" if centre_perc >= 8 else ""
            bias_segments.append(
                f'<div class="bias-segment bias-centre" style="width: {centre_perc}%">{label}</div>'
            )

        if centre_right_perc > 0:
            label = f"C-Rechts {centre_right_perc}%" if centre_right_perc >= 8 else ""
            bias_segments.append(
                f'<div class="bias-segment bias-centre-right" style="width: {centre_right_perc}%">{label}</div>'
            )

        if right_perc > 0:
            label = f"Rechts {right_perc}%" if right_perc >= 8 else ""
            bias_segments.append(
                f'<div class="bias-segment bias-right" style="width: {right_perc}%">{label}</div>'
            )

        bias_bar_html = "".join(bias_segments)

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
                        <span>{row["max_published_date"]}</span>
                    </div>
                </div>
            </div>
            
            <div class="bias-section">
                <div class="bias-bar-container">
                    {bias_bar_html}
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
    final_html = html_template.format(news_clusters="".join(clusters_html))

    # Write to file
    logging.info(f"Writing timeline HTML to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_html)

    logging.info(f"Uploading to server at {server_path}...")
    StratoUploader().upload_file(
        local_file=output_path, remote_path=server_path, overwrite=True
    )
