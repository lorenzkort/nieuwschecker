#!/usr/bin/env python3
"""
Ground News-stijl nieuwsoverzicht generator
Genereert een responsieve HTML-pagina met nieuws van de afgelopen 24 uur
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path


def generate_news_html(df: pl.DataFrame, output_path: str = "nieuws_overzicht.html", logos_folder: str = "logos"):
    """
    Genereert een responsieve HTML-pagina met nieuwsclusters
    
    Args:
        df: Polars DataFrame met nieuwsclusters
        output_path: Pad naar de output HTML-bestand
        logos_folder: Map met logo's (verwacht bestandsnamen die overeenkomen met feed-namen)
    """
    
    # Filter op artikelen van de afgelopen 24 uur
    cutoff_time = datetime.now() - timedelta(hours=24)
    df_filtered = df.filter(pl.col('max_published_date') >= cutoff_time)
    
    # Sorteer op meest recente publicatiedatum
    df_filtered = df_filtered.sort('max_published_date', descending=True)
    
    # Start HTML generatie
    html_content = generate_html_structure(df_filtered, logos_folder)
    
    # Schrijf naar bestand
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"HTML-bestand gegenereerd: {output_path}")
    print(f"Aantal nieuwsclusters: {len(df_filtered)}")


def generate_html_structure(df: pl.DataFrame, logos_folder: str) -> str:
    """Genereert de volledige HTML-structuur"""
    
    html = f"""<!DOCTYPE html>
<html lang="nl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nieuwsoverzicht - Laatste 24 uur</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f5f5f5;
            color: #333;
            line-height: 1.6;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 2rem 1rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            max-width: 1200px;
            margin: 0 auto;
            font-size: 2rem;
            font-weight: 700;
        }}
        
        .header p {{
            max-width: 1200px;
            margin: 0.5rem auto 0;
            opacity: 0.9;
            font-size: 1rem;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem 1rem;
        }}
        
        .timeline {{
            position: relative;
            padding-left: 2rem;
        }}
        
        .timeline::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 3px;
            background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
        }}
        
        .news-cluster {{
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            position: relative;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .news-cluster:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(0,0,0,0.12);
        }}
        
        .news-cluster::before {{
            content: '';
            position: absolute;
            left: -2rem;
            top: 1.5rem;
            width: 12px;
            height: 12px;
            background: #667eea;
            border-radius: 50%;
            border: 3px solid white;
            box-shadow: 0 0 0 2px #667eea;
        }}
        
        .cluster-header {{
            margin-bottom: 1rem;
        }}
        
        .cluster-title {{
            font-size: 1.5rem;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 0.5rem;
            line-height: 1.3;
        }}
        
        .cluster-meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            font-size: 0.875rem;
            color: #666;
            margin-bottom: 1rem;
        }}
        
        .meta-item {{
            display: flex;
            align-items: center;
            gap: 0.25rem;
        }}
        
        .meta-icon {{
            width: 16px;
            height: 16px;
        }}
        
        .sources-section {{
            margin-top: 1.5rem;
        }}
        
        .sources-title {{
            font-size: 0.875rem;
            font-weight: 600;
            color: #666;
            margin-bottom: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .sources-grid {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }}
        
        .source-logo {{
            width: 40px;
            height: 40px;
            border-radius: 8px;
            object-fit: contain;
            background: #f9f9f9;
            padding: 4px;
            border: 2px solid #e0e0e0;
            transition: transform 0.2s, border-color 0.2s;
        }}
        
        .source-logo:hover {{
            transform: scale(1.1);
            border-color: #667eea;
        }}
        
        .source-logo.reported {{
            border-color: #10b981;
            background: #f0fdf4;
        }}
        
        .source-logo.missing {{
            opacity: 0.3;
            border-color: #ef4444;
            background: #fef2f2;
        }}
        
        .source-fallback {{
            width: 40px;
            height: 40px;
            border-radius: 8px;
            background: #f0f0f0;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.75rem;
            font-weight: 600;
            color: #666;
            border: 2px solid #e0e0e0;
        }}
        
        .source-fallback.reported {{
            border-color: #10b981;
            background: #f0fdf4;
            color: #10b981;
        }}
        
        .source-fallback.missing {{
            opacity: 0.3;
            border-color: #ef4444;
            background: #fef2f2;
            color: #ef4444;
        }}
        
        .articles-list {{
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid #e0e0e0;
        }}
        
        .article-item {{
            padding: 0.75rem;
            margin-bottom: 0.5rem;
            background: #f9f9f9;
            border-radius: 8px;
            transition: background 0.2s;
        }}
        
        .article-item:hover {{
            background: #f0f0f0;
        }}
        
        .article-link {{
            color: #667eea;
            text-decoration: none;
            font-weight: 500;
            display: block;
            margin-bottom: 0.25rem;
        }}
        
        .article-link:hover {{
            text-decoration: underline;
        }}
        
        .article-meta {{
            font-size: 0.75rem;
            color: #999;
        }}
        
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            background: #667eea;
            color: white;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        
        .badge.timespan {{
            background: #10b981;
        }}
        
        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            color: #666;
        }}
        
        .empty-state h2 {{
            font-size: 1.5rem;
            margin-bottom: 0.5rem;
        }}
        
        @media (max-width: 768px) {{
            .header h1 {{
                font-size: 1.5rem;
            }}
            
            .timeline {{
                padding-left: 1.5rem;
            }}
            
            .news-cluster::before {{
                left: -1.5rem;
            }}
            
            .cluster-title {{
                font-size: 1.25rem;
            }}
            
            .container {{
                padding: 1rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Nieuwsoverzicht</h1>
        <p>Laatste 24 uur • Gegenereerd op {datetime.now().strftime('%d-%m-%Y om %H:%M')}</p>
    </div>
    
    <div class="container">
        <div class="timeline">
"""
    
    if len(df) == 0:
        html += """
            <div class="empty-state">
                <h2>Geen nieuws gevonden</h2>
                <p>Er zijn geen nieuwsartikelen van de afgelopen 24 uur.</p>
            </div>
"""
    else:
        # Genereer elke nieuwscluster
        for row in df.iter_rows(named=True):
            html += generate_news_cluster(row, logos_folder)
    
    html += """
        </div>
    </div>
</body>
</html>
"""
    
    return html


def generate_news_cluster(row: dict, logos_folder: str) -> str:
    """Genereert HTML voor een enkel nieuwscluster"""
    
    # Formateer datum
    max_date = row['max_published_date']
    date_str = max_date.strftime('%d %b %Y, %H:%M') if max_date else 'Onbekend'
    
    # Bereken tijd geleden
    if max_date:
        time_diff = datetime.now() - max_date.replace(tzinfo=None)
        hours_ago = int(time_diff.total_seconds() / 3600)
        if hours_ago < 1:
            time_ago = f"{int(time_diff.total_seconds() / 60)} minuten geleden"
        elif hours_ago < 24:
            time_ago = f"{hours_ago} uur geleden"
        else:
            time_ago = f"{int(hours_ago / 24)} dagen geleden"
    else:
        time_ago = ""
    
    html = f"""
            <div class="news-cluster">
                <div class="cluster-header">
                    <h2 class="cluster-title">{row['title']}</h2>
                    <div class="cluster-meta">
                        <span class="meta-item">
                            <span class="meta-icon">🕐</span>
                            <span>{time_ago}</span>
                        </span>
                        <span class="meta-item">
                            <span class="meta-icon">📄</span>
                            <span>{row['num_articles']} artikel{'en' if row['num_articles'] != 1 else ''}</span>
                        </span>
                        <span class="meta-item">
                            <span class="meta-icon">📰</span>
                            <span>{row['num_feeds']} bron{'nen' if row['num_feeds'] != 1 else ''}</span>
                        </span>
"""
    
    if row['time_span_hours'] and row['time_span_hours'] > 0:
        html += f"""
                        <span class="badge timespan">
                            Verspreid over {row['time_span_hours']:.1f} uur
                        </span>
"""
    
    html += """
                    </div>
                </div>
                
                <div class="sources-section">
                    <div class="sources-title">✅ Bronnen die dit berichtten</div>
                    <div class="sources-grid">
"""
    
    # Voeg logo's toe van bronnen die rapporteerden
    if row['feeds']:
        for feed in row['feeds']:
            html += generate_source_logo(feed, logos_folder, reported=True)
    
    html += """
                    </div>
                </div>
"""
    
    # Voeg ontbrekende bronnen toe als die er zijn
    if row['missing_feeds'] and len(row['missing_feeds']) > 0:
        html += """
                <div class="sources-section">
                    <div class="sources-title">❌ Bronnen die dit niet berichtten</div>
                    <div class="sources-grid">
"""
        for feed in row['missing_feeds']:
            html += generate_source_logo(feed, logos_folder, reported=False)
        
        html += """
                    </div>
                </div>
"""
    
    # Voeg artikellijst toe
    if row['articles'] and len(row['articles']) > 0:
        html += """
                <div class="articles-list">
"""
        for article in row['articles'][:5]:  # Toon maximaal 5 artikelen
            article_date = article['publish_date'].strftime('%H:%M') if article['publish_date'] else ''
            html += f"""
                    <div class="article-item">
                        <a href="{article['link']}" class="article-link" target="_blank" rel="noopener">
                            {article['title']}
                        </a>
                        <div class="article-meta">
                            {article['feed']} • {article_date}
                        </div>
                    </div>
"""
        
        if len(row['articles']) > 5:
            html += f"""
                    <div style="text-align: center; color: #999; font-size: 0.875rem; margin-top: 0.5rem;">
                        + {len(row['articles']) - 5} meer artikel{'en' if len(row['articles']) - 5 != 1 else ''}
                    </div>
"""
        
        html += """
                </div>
"""
    
    html += """
            </div>
"""
    
    return html


def generate_source_logo(feed: str, logos_folder: str, reported: bool = True) -> str:
    """Genereert HTML voor een bron logo"""
    
    # Probeer verschillende extensies
    logo_extensions = ['.png', '.jpg', '.jpeg', '.svg', '.webp']
    logo_path = None
    
    for ext in logo_extensions:
        potential_path = Path(logos_folder) / f"{feed}{ext}"
        if potential_path.exists():
            logo_path = potential_path
            break
    
    status_class = "reported" if reported else "missing"
    
    if logo_path:
        return f"""
                        <img src="{logo_path}" alt="{feed}" class="source-logo {status_class}" title="{feed}">
"""
    else:
        # Fallback: toon eerste letters van de feed naam
        initials = ''.join([word[0].upper() for word in feed.split()[:2]])
        return f"""
                        <div class="source-fallback {status_class}" title="{feed}">
                            {initials}
                        </div>
"""


# Voorbeeld gebruik
if __name__ == "__main__":
    import polars as pl
    df = pl.read_parquet("/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/timeline.parquet")
    
    # Genereer HTML
    generate_news_html(df, output_path="/Users/lorenzkort/Documents/LocalCode/news-data/data/visuals/nieuws_overzicht.html", logos_folder="/Users/lorenzkort/Documents/LocalCode/news-data/data/news_logos")
    print("\nHTML-bestand gegenereerd! Open 'nieuws_overzicht.html' in je browser.")
    print("\nTip: Plaats logo's in de 'logos' map met de naam van de feed (bijv. 'NOS.png', 'NU.nl.png')")