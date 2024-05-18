import os
import sys

# Obtenez le chemin absolu du répertoire du fichier en cours d'exécution
chemin_absolu = os.path.abspath(os.path.dirname(__file__))
#Ajoute du chemin courant dans la path
sys.path.append('D:\\_WS_Developpement\\Bots\\F_CryptoBotV2\\Utils')

# Affichez le chemin absolu
print("Ajouter au Path :", sys.path)