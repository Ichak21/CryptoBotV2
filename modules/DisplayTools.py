import mplfinance as mpf
import matplotlib.pyplot as plt
import pandas as pd
from statistics import mean
from .LoggerManager import LoggerManager



MODULE_NAME = "DTB - DiplayToolsBox"

PATH_SETTINGS = './DTB_Settings/'





class DisplayTools:

    def __init__(self):
        self.logs = LoggerManager()
        pass

    def _log_message(self, chart:str, messsage:str):
        return f'|{chart}|{messsage}'
    
    def select_settings(self, settings_name:str):
        #Source param pour settings : https://github.com/matplotlib/mplfinance/blob/master/examples/styles.ipynb
        #chargement du bon setting dans le fichier
        pass

    def plot_candle_pattern(self, df, levels, flags, min_grp_levels_display=2, marker_zoom=0.1):
        # Configuration des couleurs des chandeliers
        # mc = mpf.make_marketcolors(up='green', down='red', edge='inherit', wick='black', ohlc='i')

        # Configuration du style du graphique
        # s = mpf.make_mpf_style(marketcolors=mc, rc={'font.size': 12})
        style = 'binance'

        # Création de la figure et de l'axe
        chart = mpf.figure(1, figsize=(20, 7), style=style) 
        ax1 = chart.add_subplot(111)

        # Tracé du graphique de chandeliers
        mpf.plot(df, type='candle', ax=ax1)

        if len(df.index) == len(flags):
            for index, marker in enumerate(flags):
                # Marquage d'un point spécifique sur le graphique
                if marker == 'v' :
                    plt.scatter(index, df.iloc[index]["high"] *(1 + marker_zoom), s=50, marker=marker, c='red', zorder=2)

                if marker == '^' :
                    plt.scatter(index, df.iloc[index]["low"] * (1 - marker_zoom), s=50, marker=marker, c='green', zorder=2)

                if marker == '1' :
                    plt.scatter(index, df.iloc[index]["high"] *(1 + marker_zoom), s=50, marker=marker, c='red', zorder=2)

                if marker == '2' :
                    plt.scatter(index, df.iloc[index]["low"] * (1 - marker_zoom), s=50, marker=marker, c='green', zorder=2)

                if marker == '.' :
                    plt.scatter(index, df.iloc[index]["low"]+(df.iloc[index]["high"]-df.iloc[index]["low"]) * (1 - marker_zoom), s=50, marker=marker, c='yellow', zorder=2)
                    
                #could be implemented
                # '.' : Point
                # ',' : Pixel
                # 'o' : Cercle
                # 'v' : Triangle vers le bas
                # '^' : Triangle vers le haut
                # '<' : Triangle vers la gauche
                # '>' : Triangle vers la droite
                # '1' : Pointe vers le bas
                # '2' : Pointe vers le haut
                # '3' : Pointe vers la gauche
                # '4' : Pointe vers la droite
                # 's' : Carré
                # 'p' : Pentagone
                # '*' : Étoile
                # 'h' : Hexagone1
                # 'H' : Hexagone2
                # '+' : Plus
                # 'x' : Croix
                # 'D' : Losange
                # 'd' : Petit losange
        else :
            self.logs.log_error(MODULE_NAME,self._log_message("Candle W Flags", "list of marker not align with ochlv datas !"))
        
        if levels:
            for level in levels:
                if len(level) > min_grp_levels_display:
                    plt.hlines(mean(level), xmin=0, xmax=len(df), colors='lightcoral', lw=len(level)-min_grp_levels_display)

        # Affichage du graphique
        plt.show()

