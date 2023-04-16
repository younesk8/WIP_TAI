# Squelette de projet Traitement de l'Intelligence Artificielle

Le but de ce projet est de mettre en oeuvre une solution IA associé à l'IoT et le Big Data.
Le squelette est adapté à la classe I1 GR4

Les élèves utilisent tous Kafka pour envoyer les données du capteur vers un cluster Spark.

Il y a les programmes suivants :
1. Un Capteur
2. Un programme pour tourner Kafka
3. Un programme pour nettoyer les données de Kafka via Spark
4. Un programme pour entraîner un modèle de machine learning
5. Un programme pour prédire les résultats à partir du modèle entraîné

Rendu :
- Le code source
- Un rapport
- Eventuellement un support de présentation au format PDF

Pour installer Kafka : Deux choix:
- Soit avec Conduktor (Le plus simple)
- Soit avec Docker-compose (Plus compliqué).

Pour Hadoop, je vous conseil la méthode Docker qui est moins casse-tête à installer
(Les instructions vont venir au fil de l'eau, le temps de que je complète le code)