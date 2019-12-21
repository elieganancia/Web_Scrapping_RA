# Project Title

This Data-Mining project for ITC Fellow Data Science 2019-2020 is dedicated to Scrapp most of the important Data on the website Resident Advisor.

## About the Project

This program will collect everything you'll ever need or want to know about Nightlife all aroud the world. We scrapped thousands of events every week and help you choose which one to go to ! 
We added to this event databases, multiple details about Clubs, Artists, their Labels .. and also what the do if you don't know them ! 

BREAKING NEWS !! We added the Meteo and the Spotify API !
You'll be able to listen to some music or get to know better an artist you'll see tonight ! Or maybe not tonight.. now that we have the Meteo ;) 


### Prerequisites

You'll need to install all packages on the requirement file 

```
pip install -r requirements.txt
```

### Installing

A step by step series of examples that tell you how to get a development env running

First, let's clone our git repository into your computer..

```
git clone https://github.com/elieganancia/Web_Scrapping_RA.git
```

And then just go with it using to know what it can do !

```
python main blabla
```

End with an example of getting some data out of the system or using it for a little demo

```
>>>> python main.py blabla
usage: main_scrapping.py [-scrap_labels] [-scrap_artists] [-scrap_events] [-scrap_clubs] [-get_csv] 
 /// This program will ask to Scrappy Coco to get some information from the website Resident Advisor. 
 If the user do not specify options, it will scrap everything and store it in a database. 
Option Description : 
-scrap_labels : specify this option if you want to scrap labels 
-scrap_artists : specify this option if you want to scrap artists 
-scrap_events : specify this option if you want to scrap events 
-scrap_clubs : specify this option if you want to scrap clubs 
-erase_database : specify this option if you want to delete your 
-get_external_data : specify this option if you want to add external data such as spotify data and meteo data 
resident advisor database 

 Usage example : (1) main_scrapping.py -scrap_labels -scrap_artists 
 (2) main_scrapping.py 
(3) main_scrapping.py -scrap_labels -get_csv /// 
main.py: error: unrecognized arguments: blabla
```

## Deployment

We deployed it on AWS EC2.

## Built With

* [Pycharm](https://www.jetbrains.com/fr-fr/pycharm/) - The Python Framework used
* [MYSQL](https://www.mysql.com/fr/products/connector/) - Database Connector
* [AWS](https://aws.amazon.com/fr/) - Deployment 

## Contributing

Nobody ! We are two grown up guys ! 

## Versioning

Version 1.0

## Authors

* **Pierre Chemla** - *Initial work* - [ITC](https://github.com/elieganancia/Web_Scrapping_RA)

* **Elie Ganancia** - *Initial work* - [ITC](https://github.com/elieganancia/Web_Scrapping_RA)


## License

This project is licensed under the ITC License




