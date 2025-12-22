# Project Description
Goal: Covering real-time News Coverage Assymmetries

MVP Phase 1:
- [x] Ingest Title's and content of news articles every hour for NOS.nl
- [x] Ingest Title's and content of news articles every hour for AD.nl
- [x] Figure out how to extract topics and match them to other articles: use simple NLP-libraries, no LLM's (yet)
- [ ] Categorise into topics

MVP Phase 2:
- Ingest Title's and content of news articles every hour for:
    - NOS
    - NRC
    - de Volkskrant
    - Trouw
    - AD
    - Telegraaf
    - FD
    - RTL Nieuws
    - NU.nl
    - ...
- Manually define 3–4 clusters, clearly documented:
    - Public / institutional
    - Progressive / centre-left
    - Liberal / centre-right
    - Commercial / populist
    - ...
- Pick 5–8 high-salience, recurring domains, like:
    - Immigration & asylum
    - Housing
    - Climate & energy
    - Healthcare
    - Public spending & taxation
    - Crime & safety
    - Corporate regulation
    - Agriculture / nitrogen
    - ...
- For each topic, per 24 hours:
    - Number of articles per outlet
    - Normalised by total output
    - Compared across clusters
- Final Feature:
    - “Today, {popular theme} was covered by {news outlets}, but absent from {news outlets}. 