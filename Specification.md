# Analiza podataka letova aviona
---
# Domen
 - Domen kome pripada aplikacija je avionska industrija
 - Aplikacija ce da obradjuje podatke o letovima i aerodromima
---
# Motivacija
 - Motivacija za bavljenjem ovom temom mi je bila da dobijem veci uvid u to kako rade avionske kompanije i da pomognem osoblju koje radi na aerodromima
 - Takodje ljubav prema visini i letenju
---
# Ciljevi
 - Olaksavanje uvida u veoma veliki skup podatak koji generisu avionske kompanije
 - Olaksavanje posla osoblju na aerodromima
 - Mogucnost obicnih korisnika avionskih kompanija da imaju uvid u avione koji lete i koji su leteli
---
# Skup podataka za batch obradu
 - Skup podataka: https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022
 - Skup podataka sadrzi letove civilnih aviona iznad Sjedinjenih AMerickih Drzava u periodu od januara 2018 do jula 2022 godine
 - Za batch obradu na ovom projektu ce se iskoristiti podskup ovog skupa, zbog prevelike velicine citavog skupa (>25GB)
 - Neke od kolana koje su bitne za batch obradu podataka: Identifikacioni broj leta, Datum leta, identifikacioni broj aviona, avionska kompanija kojoj pripada avion, aerodrom sa kog polece avion, aerodrom na koji slece avion, da li je let otkazan, da li je let promenio odrediste, zakazano vreme polaska, vreme polaska, zakazano vreme dolaska, vreme dolaska, TaxiIn vreme, TaxiOut vreme...
---
# Pitanja batch obrade
 1. Koliko je svaki od aviona proveo u vazduhu?
 2. Koliko je svaka od avionskih kompanije imala airtime na svojim avionima?
 3. Koji aerodromi su imali najveci dolazni saobracaj?
 4. Koji aerodromi su imali najveci odlazni saobracaj?
 5. Na kom aerodromu je bilo najvece prosecno kasnjenje?
 6. Koliko je svaki avion presao kilometara?
 7. Kojim danom je bilo najvise letova?
 8. Na kom aerodromu je bilo najvise otkazanih letova?
 9. Na kojoj destinaciji je bilo najvise preusmerenih letova?
 10. Avionska kompanija sa najmanje minuta kasnjenja?
 11. Koji put izmedju dva aerodroma je imao najmanje kasnjenja?
 12. Koju aerodrom ima najmanje TaxiIn/TaxiOut vreme?
---
# Skup podataka za real time obradu podataka
 - Real time API dokumentacija: https://openskynetwork.github.io/opensky-api/rest.html
 - Ovaj API dozvoljava dnevno 4000 kredita za upite na njihov API
 - Moguce je ograniciti geografsku sirinu i duzinu letece objekte koji se dobavljaju
 - Pomenuti API nam odgovara u JSON formatu sa dva polja, `time` koje predstavlja trenutno vreme i `states` lista stanja svih letecih objekata koji upadaju u zatrazeni region
 - Neke od stvari od kojih se sastoji stanje leteceg objekta su: identifikator, odzivni broj, zemlja porekla, vremenska dobjianja poslednjeg azuriranja pozicije, geografska sirina, geografska duzinu, nadmorska visina, da li je objekat na zemlji, brzina...
 ---
# Pitanja za real time obradu podataka
 1. Koji je najblizi aerodrom datom avionu?
 2. Koji avion je imao najvece promene u visini u poslednjih 2 min?
 3. Da li se neki avion prizemljio u poslednjih 2 min?
 4. Koja je najveca zabelezena brzina u poslednjih 2 min?
 5. Na kom delu neba je najvise aviona?