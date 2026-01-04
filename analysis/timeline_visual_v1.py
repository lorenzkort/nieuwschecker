#!/usr/bin/env python3
"""
Generate a Ground News-style responsive HTML interface from news cluster data.
Expects a Polars dataframe with news clusters and creates an interactive timeline view.
"""

import polars as pl
from datetime import datetime
from pathlib import Path


def generate_news_html(df: pl.DataFrame, output_path: str = "news_timeline.html", logos_folder: str = "logos"):
    """
    Generate a responsive HTML news interface from a Polars dataframe.
    
    Args:
        df: Polars dataframe with news cluster data
        output_path: Path where the HTML file will be saved
        logos_folder: Folder containing news source logos (named as feed names with extensions)
    """
    
    # Sort by most recent first
    df = df.sort("max_published_date", descending=True)
    
    # Generate HTML content
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>News Timeline - Last 24 Hours</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        header {{
            background: white;
            padding: 30px;
            border-radius: 16px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            margin-bottom: 30px;
            text-align: center;
        }}
        
        h1 {{
            color: #2d3748;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .subtitle {{
            color: #718096;
            font-size: 1.1em;
        }}
        
        .timeline {{
            position: relative;
            padding-left: 40px;
        }}
        
        .timeline::before {{
            content: '';
            position: absolute;
            left: 20px;
            top: 0;
            bottom: 0;
            width: 3px;
            background: linear-gradient(to bottom, #667eea, #764ba2);
        }}
        
        .news-cluster {{
            background: white;
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 30px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.08);
            position: relative;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .news-cluster:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 25px rgba(0,0,0,0.12);
        }}
        
        .news-cluster::before {{
            content: '';
            position: absolute;
            left: -33px;
            top: 35px;
            width: 16px;
            height: 16px;
            border-radius: 50%;
            background: #667eea;
            border: 3px solid white;
            box-shadow: 0 0 0 3px #667eea;
        }}
        
        .cluster-header {{
            margin-bottom: 20px;
        }}
        
        .cluster-title {{
            font-size: 1.5em;
            color: #2d3748;
            margin-bottom: 10px;
            line-height: 1.4;
        }}
        
        .cluster-meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            color: #718096;
            font-size: 0.9em;
            margin-bottom: 15px;
        }}
        
        .meta-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        
        .meta-icon {{
            font-weight: bold;
        }}
        
        .sources-section {{
            margin-top: 20px;
        }}
        
        .sources-label {{
            font-weight: 600;
            color: #4a5568;
            margin-bottom: 10px;
            font-size: 0.95em;
        }}
        
        .sources-grid {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-bottom: 15px;
        }}
        
        .source-logo {{
            width: 48px;
            height: 48px;
            border-radius: 8px;
            object-fit: contain;
            background: #f7fafc;
            padding: 6px;
            border: 2px solid #e2e8f0;
            transition: transform 0.2s, border-color 0.2s;
            cursor: pointer;
        }}
        
        .source-logo:hover {{
            transform: scale(1.1);
            border-color: #667eea;
        }}
        
        .source-logo.reported {{
            border-color: #48bb78;
            background: #f0fff4;
        }}
        
        .source-logo.missing {{
            opacity: 0.3;
            border-color: #fc8181;
            background: #fff5f5;
        }}
        
        .source-fallback {{
            width: 48px;
            height: 48px;
            border-radius: 8px;
            background: #edf2f7;
            border: 2px solid #e2e8f0;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.75em;
            color: #4a5568;
            text-align: center;
            padding: 4px;
            line-height: 1.2;
        }}
        
        .source-fallback.reported {{
            border-color: #48bb78;
            background: #f0fff4;
            color: #22543d;
        }}
        
        .source-fallback.missing {{
            opacity: 0.3;
            border-color: #fc8181;
            background: #fff5f5;
        }}
        
        .articles-list {{
            margin-top: 15px;
            padding-top: 15px;
            border-top: 1px solid #e2e8f0;
        }}
        
        .article-item {{
            padding: 10px;
            margin-bottom: 8px;
            background: #f7fafc;
            border-radius: 6px;
            transition: background 0.2s;
        }}
        
        .article-item:hover {{
            background: #edf2f7;
        }}
        
        .article-link {{
            color: #667eea;
            text-decoration: none;
            font-weight: 500;
            display: block;
            margin-bottom: 5px;
        }}
        
        .article-link:hover {{
            text-decoration: underline;
        }}
        
        .article-meta {{
            font-size: 0.85em;
            color: #718096;
        }}
        
        .stats-bar {{
            background: #f7fafc;
            padding: 15px;
            border-radius: 8px;
            margin-top: 15px;
            display: flex;
            justify-content: space-around;
            flex-wrap: wrap;
            gap: 15px;
        }}
        
        .stat-item {{
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 1.5em;
            font-weight: 700;
            color: #667eea;
        }}
        
        .stat-label {{
            font-size: 0.85em;
            color: #718096;
            margin-top: 5px;
        }}
        
        .toggle-articles {{
            background: #667eea;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9em;
            margin-top: 10px;
            transition: background 0.2s;
        }}
        
        .toggle-articles:hover {{
            background: #5568d3;
        }}
        
        .articles-list.collapsed {{
            display: none;
        }}
        
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            h1 {{
                font-size: 1.8em;
            }}
            
            .timeline {{
                padding-left: 25px;
            }}
            
            .timeline::before {{
                left: 10px;
            }}
            
            .news-cluster::before {{
                left: -23px;
                width: 12px;
                height: 12px;
            }}
            
            .cluster-title {{
                font-size: 1.2em;
            }}
            
            .source-logo, .source-fallback {{
                width: 40px;
                height: 40px;
            }}
        }}
        
        .legend {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        }}
        
        .legend-title {{
            font-weight: 600;
            margin-bottom: 10px;
            color: #2d3748;
        }}
        
        .legend-items {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 0.9em;
            color: #4a5568;
        }}
        
        .legend-box {{
            width: 24px;
            height: 24px;
            border-radius: 4px;
            border: 2px solid;
        }}
        
        .legend-box.reported {{
            border-color: #48bb78;
            background: #f0fff4;
        }}
        
        .legend-box.missing {{
            opacity: 0.3;
            border-color: #fc8181;
            background: #fff5f5;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📰 News Timeline</h1>
            <div class="subtitle">Last 24 Hours Coverage</div>
        </header>
        
        <div class="legend">
            <div class="legend-title">Source Coverage Legend</div>
            <div class="legend-items">
                <div class="legend-item">
                    <div class="legend-box reported"></div>
                    <span>Reported by source</span>
                </div>
                <div class="legend-item">
                    <div class="legend-box missing"></div>
                    <span>Not covered by source</span>
                </div>
            </div>
        </div>
        
        <div class="timeline">
"""
    
    # Generate news clusters
    for row in df.iter_rows(named=True):
        cluster_id = row['cluster_id']
        title = row['title']
        articles = row['articles']
        num_articles = row['num_articles']
        feeds = row['feeds']
        num_feeds = row['num_feeds']
        min_date = row['min_published_date']
        max_date = row['max_published_date']
        time_span = row['time_span_hours']
        missing_feeds = row['missing_feeds']
        
        # Format dates
        if isinstance(min_date, datetime):
            min_date_str = min_date.strftime("%B %d, %Y %H:%M")
            max_date_str = max_date.strftime("%B %d, %Y %H:%M")
        else:
            min_date_str = str(min_date)
            max_date_str = str(max_date)
        
        html_content += f"""
            <div class="news-cluster">
                <div class="cluster-header">
                    <h2 class="cluster-title">{title}</h2>
                    <div class="cluster-meta">
                        <div class="meta-item">
                            <span class="meta-icon">📅</span>
                            <span>{max_date_str}</span>
                        </div>
                        <div class="meta-item">
                            <span class="meta-icon">⏱️</span>
                            <span>{time_span:.1f} hours span</span>
                        </div>
                    </div>
                </div>
                
                <div class="stats-bar">
                    <div class="stat-item">
                        <div class="stat-value">{num_articles}</div>
                        <div class="stat-label">Articles</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{num_feeds}</div>
                        <div class="stat-label">Sources Covered</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{len(missing_feeds) if missing_feeds else 0}</div>
                        <div class="stat-label">Sources Missed</div>
                    </div>
                </div>
                
                <div class="sources-section">
                    <div class="sources-label">✅ Sources that reported:</div>
                    <div class="sources-grid">
"""
        
        # Add source logos for feeds that reported
        if feeds:
            for feed in feeds:
                logo_path = f"{logos_folder}/{feed}.png"
                html_content += f"""
                        <img src="{logo_path}" 
                             alt="{feed}" 
                             title="{feed}" 
                             class="source-logo reported"
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                        <div class="source-fallback reported" style="display:none;">{feed[:8]}</div>
"""
        
        html_content += """
                    </div>
                </div>
"""
        
        # Add missing sources if any
        if missing_feeds and len(missing_feeds) > 0:
            html_content += """
                <div class="sources-section">
                    <div class="sources-label">❌ Sources that didn't cover:</div>
                    <div class="sources-grid">
"""
            for feed in missing_feeds:
                logo_path = f"{logos_folder}/{feed}.png"
                html_content += f"""
                        <img src="{logo_path}" 
                             alt="{feed}" 
                             title="{feed} (did not cover)" 
                             class="source-logo missing"
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                        <div class="source-fallback missing" style="display:none;">{feed[:8]}</div>
"""
            html_content += """
                    </div>
                </div>
"""
        
        # Add articles list
        html_content += f"""
                <button class="toggle-articles" onclick="toggleArticles({cluster_id})">
                    Show All {num_articles} Articles
                </button>
                
                <div id="articles-{cluster_id}" class="articles-list collapsed">
"""
        
        if articles:
            for article in articles:
                article_title = article['title']
                article_link = article['link']
                article_feed = article['feed']
                article_date = article['publish_date']
                
                if isinstance(article_date, datetime):
                    article_date_str = article_date.strftime("%B %d, %Y %H:%M")
                else:
                    article_date_str = str(article_date)
                
                html_content += f"""
                    <div class="article-item">
                        <a href="{article_link}" target="_blank" class="article-link">
                            {article_title}
                        </a>
                        <div class="article-meta">
                            {article_feed} • {article_date_str}
                        </div>
                    </div>
"""
        
        html_content += """
                </div>
            </div>
"""
    
    # Close HTML
    html_content += """
        </div>
    </div>
    
    <script>
        function toggleArticles(clusterId) {
            const articlesList = document.getElementById(`articles-${clusterId}`);
            const button = event.target;
            
            if (articlesList.classList.contains('collapsed')) {
                articlesList.classList.remove('collapsed');
                button.textContent = 'Hide Articles';
            } else {
                articlesList.classList.add('collapsed');
                const numArticles = articlesList.querySelectorAll('.article-item').length;
                button.textContent = `Show All ${numArticles} Articles`;
            }
        }
    </script>
</body>
</html>
"""
    
    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Generated news timeline HTML at: {output_path}")
    return output_path


# Example usage
if __name__ == "__main__":
    # Example: Create sample data (replace with your actual dataframe)
    df = pl.read_parquet("/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/timeline.parquet")
    generate_news_html(df, "/Users/lorenzkort/Documents/LocalCode/news-data/data/visuals/news_timeline.html",
                       "/Users/lorenzkort/Documents/LocalCode/news-data/data/news_logos")
    
    print("News Timeline Generator")
    print("=" * 50)
    print("\nUsage:")
    print("  from generate_news_interface import generate_news_html")
    print("  import polars as pl")
    print("")
    print("  df = pl.read_parquet('your_data.parquet')")
    print("  generate_news_html(df, 'output.html', 'logos_folder')")
    print("\nMake sure you have a 'logos' folder with PNG images named after your feeds!")