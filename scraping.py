import time
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np

inizio = time.time()
def controlled(link):
    # link: string
    # RETURN the link with the missing part
    if link[:4] != 'http':
        link = 'https://www.immobiliare.it' + link
    return link

n = 1000
def scrape_home_page_annunci(comune = "milano",n = n):
    # Init params and links list
    links = []
    idx = 1
    count = 0
    # Iterate over n
    while (len(links) < n):
        # Creazione link e request
        url = f'https://www.immobiliare.it/vendita-case/{comune}/?criterio=rilevanza&pag=' + str(idx)
        idx += 1
        content = requests.get(url)
        soup = BeautifulSoup(content.text, "lxml")
        if count == 100:
            time.sleep(1)
            count = 0
        # Estrazione degli link annunci su homepage
        divTag = soup.find_all("div", {'class': "nd-mediaObject__content in-card__content in-realEstateListCard__content"})
        for tag in divTag:
            tdTags = tag.find_all("a")
            for tag in tdTags:
                l = controlled(tag['href'])
                links.append(l)
    return links


links = pd.Series(scrape_home_page_annunci())


def scrape_annuncio(url):
    # Init
    info_annuncio = pd.Series(dtype = "object",name = url)

    # Link annuncio
    info_annuncio["link"] = url

    # Estrai id annuncio
    info_annuncio["id_annuncio"] = url.split("/")[-2]

    # Estrazione raw info da url
    content = requests.get(url)
    soup = BeautifulSoup(content.text, "lxml")

    # Estrai titolo
    info_annuncio["titolo"] = soup.find("span", {'class': "im-titleBlock__title"}).text

    # Estrai via
    divTag = soup.find_all("span", {'class': "im-location"})
    # trucco di codice per estrarre info
    for n, info in enumerate(divTag[::-1]):
        if n == 0:
            info_annuncio["via"] = info.text

    # Estrai descrizione
    divTag = soup.find_all("div", {'class': "im-readAll__container im-readAll__container--lessContent js-readAllContainer"})
    info_annuncio["descrizione"] = " ".join([i.text.replace("\n","").replace("  ","") for i in divTag])

    # Estrai offerente
    divTag = soup.find_all("div", {'class': "im-lead__reference"})
    info_annuncio["offerente"] = " ".join([i.find("p").text for i in divTag][-1])

    # Estrai caratteristiche
    divTag = soup.find_all("dd", {'class': "im-features__value"})
    values = [i.text.replace("\n","").replace("  ","") for i in divTag]
    divTag = soup.find_all("dt", {'class': "im-features__title"})
    title =  [i.text.replace("\n","").replace("  ","") for i in divTag]
    caratteristiche = pd.Series({ t:v for t,v in zip(title,values)})
    info_annuncio = pd.concat([info_annuncio,caratteristiche],axis=0 )

    # Estrai id unita abitative
    divTag = soup.find_all("li", {'class':  "nd-list__item im-properties__item js-units-track"})
    info_annuncio["id_unita_abitativa"] = [ i["data-track-id"]  for i in divTag]

    # Link unita abitative
    info_annuncio["link_unita_abitative"] = [ url+i for i in info_annuncio["id_unita_abitativa"] ]


    return info_annuncio

# to check
def scrape_unita_da_progetto(url):
    try:
        content = requests.get(url)
        soup = BeautifulSoup(content.text, "lxml")

        # Estrazione link unita abitative
        divTag = soup.find_all("a", {'class': "im-properties__summary"})
        unita = []
        non_valid = ["€", ""]
        for info in divTag:
            unita.append([ i for i in info.text.replace("\n","").split(" ") if i not in non_valid])

        df_unita = pd.DataFrame(unita)
        df_unita = df_unita.iloc[:,[0,1,2,4,6]]
        df_unita.columns = ["tipologia_unita","prezzo","locali","superficie_m2","bagni"]
        df_unita["id_annuncio"] = url.split("/")[-2]
    except:
        print(f"Failed:{url}")
    return  df_unita


def join_progetti_unita(url):
    try:
        return scrape_unita_da_progetto(url).merge(pd.DataFrame(scrape_annuncio(url)).T)
    except:
        print("Join Failed")
        pass

# Run
total_df = pd.concat([ scrape_annuncio(url) for url in links],axis = 1).T
# Normalize columns name lower case
total_df.columns = [ i.lower() for i in total_df.columns.tolist() ]
progetti = total_df.loc[total_df["tipologia"] == "Progetto"]
progetti_links = progetti["link"].tolist()
scraping_totale_progetti = pd.concat([join_progetti_unita(url) for url in progetti_links], axis = 0 ).reset_index(drop = True)


# Save
total_df.to_csv("data/annunci_immobiliare_milano.csv",index=False,header=True)
progetti.to_csv("data/progetti_immobiliare_milano.csv",index=False,header=True)
scraping_totale_progetti.to_csv("data/progetti_unita_immobiliare_milano.csv",index=False,header=True)

fine = time.time()
tempo = fine - inizio
print(f"Scaricato in {round(tempo,2)} secondi {n} annunci!")
# 340 secondi 1000 annunci immobiliari
