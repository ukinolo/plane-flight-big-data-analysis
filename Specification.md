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
 1. Koliko je svaki od aviona proveo u vazduhu, presao kilometara i imao letova?
 2. Koliko je svaka od avionskih kompanije imala airtime na svojim avionima, presla ukupno kilometara i imala broj letova?
 3. Broj polazaka i dolazaka aviona na svakom od aerodroma tokom godina?
 4. Raspored broja letova po godinama?
 5. Pomerajuce prosecno nedeljno kasnjenje za svaki avio kompaniju?
 6. TaxiIn i TaxiOut vreme po danima na aerodromima?
 7. Broj aviona u vazduhu, po danu i satu?
 8. Top 5 najbrzih letova izmedju svaka dva aerodroma?
 9. Prosecno godisnje kasnjenje po danima u nedelju po aerodromu.
 10. Broj aktivnih aviona avionskih kompanija po mesecima.
---
# Skup podataka za real time obradu podataka
 - Real time API dokumentacija: https://openskynetwork.github.io/opensky-api/rest.html
 - Ovaj API dozvoljava dnevno 4000 kredita za upite na njihov API
 - Moguce je ograniciti geografsku sirinu i duzinu letece objekte koji se dobavljaju
 - Pomenuti API nam odgovara u JSON formatu sa dva polja, `time` koje predstavlja trenutno vreme i `states` lista stanja svih letecih objekata koji upadaju u zatrazeni region
 - Neke od stvari od kojih se sastoji stanje leteceg objekta su: identifikator, odzivni broj, zemlja porekla, vremenska dobjianja poslednjeg azuriranja pozicije, geografska sirina, geografska duzinu, nadmorska visina, da li je objekat na zemlji, brzina...
 ---
# Pitanja za real time obradu podataka
 1. Koliko se trenutno aviona nalazi u vazduhu?
 2. Kolika ja maksimalna brzina aviona u prethodna 2 minuta?
 3. Koliko je prosecna promene visine aviona u prethodna 2 minuta?
 4. Koliki je broj aviona u svakoj od visinskih grupa u prethodna 2 minuta?
 5. Koliko je aviona u svakoj od brzinskih grupa u prethodna 2 minuta?