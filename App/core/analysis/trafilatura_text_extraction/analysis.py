# python3

import trafilatura

# AD -> None
url = "https://www.ad.nl/buitenland/witte-huis-sluit-inzet-leger-niet-uit-bij-verkrijgen-groenland-al-houdt-minister-het-op-kopen~a7975ae2/"

# NOS -> Works!
url = "https://nos.nl/l/2597904"

# NU.nl -> None
url = "https://www.nu.nl/binnenland/6383127/lichaam-tijn-25-volgens-om-verbrand-in-olievat-bruut-en-onmenselijk.html"

# Volkskrant -> Works! Great way to add the article summaries to the process
url = "https://www.volkskrant.nl/columns-van-de-dag/aan-de-meubels-kleeft-een-klinische-gedwongen-vtwonen-achtige-spanning~b9829656/"

# Telegraaf -> None
url = "https://www.telegraaf.nl/binnenland/live-marokko-fans-in-amsterdam-geloven-nog-in-de-overwinning-in-afrika-cup/124602227.html"

# FD -> Bullet points which add a bit to the article, but not much
url = "https://fd.nl/bedrijfsleven/1584390/moederbedrijf-batavus-en-sparta-heeft-weer-geld-nodig"

# NRC -> Works! Basically the whole article available
url = "https://www.nrc.nl/nieuws/2026/01/09/de-grote-afbrokkeling-is-begonnen-a4917208"

# Trouw -> Works! Basically the whole article available
url = "https://www.trouw.nl/duurzaamheid-economie/hoe-de-duurzame-boer-in-2025-juist-niet-beloond-werd-of-toch-wel-geluid-maken-helpt~b6094577/"

# BNR -> None
url = "https://www.bnr.nl/nieuws/internationaal/10591824/iraanse-oud-kroonprins-roept-op-bereid-je-voor-om-steden-in-te-nemen"

# De gelderlander -> None
url = "https://www.gelderlander.nl/economie/topbestuurders-eisen-miljardenfonds-vertraging-nieuwe-energie-infrastructuur-bedreigt-banen~a7c40088/"

# ED -> None
url = "https://www.ed.nl/buitenland/sydney-sluit-stranden-na-derde-haaienaanval-in-twee-dagen-tijd~a290cf50/"

# bd -> None
url = "https://www.bd.nl/buitenland/oostenrijk-in-rep-en-roer-kalfsvlees-voor-wienerschnitzel-komt-uit-nederland-zonder-dat-consument-dat-weet~a4a1c70a/"

# Destentor -> None
url = "https://www.destentor.nl/buitenland/kijk-britse-bestuurder-is-zo-dronken-dat-hij-acht-pogingen-nodig-heeft-om-te-blazen~acf05a2d/"

# Parool -> Works! Great way to add an article summary
url = "https://www.parool.nl/nederland/oud-en-nieuw-wordt-bewolkt-maar-grotendeels-droog~b7b392ca/"

page = trafilatura.fetch_url(url=url)

text = trafilatura.extract(filecontent=page)

print(text)

# ---------------- CONCLUSION ------------------
# Great way to add summaries to where they are not scraped (parool, trouw, volkskrant), and potential to get whole article from NRC
