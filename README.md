<img src='./src/LoLlogo.png'>

# Project LoL

Name : Iandro Rakotondrandria

Slides: https://docs.google.com/presentation/d/1P3jYyhB-PNOWHkULncVYaCYij1e4uy-5/edit?usp=sharing&ouid=111559984624113307312&rtpof=true&sd=true

## Context 📇

**League of Legends** is a very popular team-based, multiplayer battle arena video game with over 160 champions to play with, developed and published by Riot Games.

To put things in perspective, here are some numbers about the game. LoL (League of Legends) has:
* over 125 millions active players every month
* over 700 000 active players at the same time
* over 1 billion hours of watchtime for its 2021 world champonship
* 2,2 millions dollars prize pool distributed in that championship

The most prominent game mode is the Summoner's Rift. The mode has a ranked competitive ladder; a matchmaking system determines a player's skill level and generates a starting rank from which they can climb. There are nine tiers, which are, from the least skilled to the highest :
- Iron (with 2.2% of the players)
- Bronze (with 20% of the players)
- Silver (with 37% of the players)
- Gold (with 27% of the players)
- Platinum (with 9.8% of the players)
- Diamond (with 1.5% of the players)
- Master (with 0.12% of the players)
- GrandMaster (with 0.031% of the players)
- Challenger (with 0.013% of the players)

Our focus here will be on the Iron and Diamond tiers, since it's the entry level and there are too few players above Diamond.


## Goals 🎯

The goal here is to provide, to an entry level Iron division player, relevant visualisations and statistics from up to date data from his division and the Diamond division, to help him build the best strategy to efficently climb to the Diamond rank.

To get there, we will be :

* collecting data from RiotGame's API
* consuming and processing data with Kafka and Confluent Cloud
* storing processed data in a PostgreSQL database in Amazon RDS
* visualizing results in a dashboard


## Deliverables 📬

To complete this project, we produced:

* A schema of the infrastructure we built and why we built it that way
    * in a Google Slide

* The source code of all elements necessary to build our infrastructure