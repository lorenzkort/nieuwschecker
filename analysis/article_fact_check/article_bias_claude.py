import anthropic
import json
import os

client = anthropic.Anthropic(
    api_key=os.environ.get("CLAUDE_API_KEY")
)

# Create a message with a system prompt
message = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1024,
    system=open("/Users/lorenzkort/Documents/LocalCode/news-data/analysis/article_bias/fact_checking_sys_prompt_NL.md", 'r').read(),
    messages=[
        {
            "role": "user",
            "content": '''Feitencontrole uitvoeren: De voormalige kroonprins van Iran heeft vanuit ballingschap in de Verenigde Staten demonstranten opgeroepen om "stadscentra in te nemen" en tot een nationale staking. Volgens een mensenrechtengroep zijn al 65 Iraniërs omgekomen tijdens de protestgolf.
"Ons doel is niet langer alleen maar de straat op te gaan; het doel is ons voor te bereiden om stadscentra te veroveren en te behouden", schreef Reza Pahlavi op X. Hij is de zoon van de laatste Iraanse sjah en was tot de Islamitische Revolutie in 1979 de beoogde troonopvolger.

Een deel van de demonstranten rekent op de terugkeer van Pahlavi en ziet hem als beter alternatief voor het geestelijke leiderschap onder ayatollah Khamenei. De oppositieleider had ook de voorbije dagen al online opgeroepen tot demonstraties.

Volgens Iran-expert Damon Golriz van het Haags Instituut GeopolitiekNu associëren veel Iraniërs Pahlavi met "een nostalgie naar een verleden waarin het beter ging in Iran, en ook de relaties met de VS en Israël goed waren". Het draagvlak voor Pahlavi is de laatste tijd groter geworden, stelt Golriz.

De protesten in Iran duren inmiddels al twee weken en zijn de laatste dagen in intensiteit toegenomen. Het regime in Teheran dreigde "opruiende en onruststokende elementen" zwaar te straffen, waarmee verwezen werd naar de demonstranten.

Al 65 Iraniërs omgekomen tijdens protestgolf
Donderdag werd het internet in grote delen van Iran uitgeschakeld. Mogelijk was dat een reactie op de oproepen van Pahlavi. In het verleden gingen die onderbrekingen van het internet gepaard met hard optreden van het regime tegen demonstranten.

Volgens de Iraanse mensenrechtengroep HRANA zijn al 65 Iraniërs omgekomen tijdens de protestgolf. Het zou gaan om vijftig burgers en vijftien leden van de veiligheidsdiensten. Volgens de Noorse rechtengroep Hengaw zijn inmiddels 2.500 mensen gearresteerd.

Ook de Amerikaanse president Donald Trump bemoeide zich met de demonstraties in Iran. Hij dreigde met een Amerikaanse tussenkomst als het regime zich op burgers zou richten. "Je kunt maar beter niet beginnen met schieten, want dan beginnen wij ook te schieten", zei hij vrijdag tegen verslaggevers in het Witte Huis.'''
        }
    ]
)

# Extract the text content
response_text = message.content[0].text
print(response_text)

# Parse the JSON response
try:
    json_response = json.loads(response_text)
    print(json.dumps(json_response, indent=2))
except json.JSONDecodeError as e:
    print(f"Failed to parse JSON: {e}")
    print(f"Raw response: {response_text}")