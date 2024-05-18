import pandas as pd
from .Utiles import _shift_data_in_columns
from .LoggerManager import LoggerManager



MODULE_NAME = "ATB - DiplayToolsBox"

PATH_SETTINGS = './ATB_Settings/'


class AnalyzerTools:

    def __init__(self):
        self.logs = LoggerManager()
        pass
    
    def _trend_get_top_botom(self, ohlv:pd.DataFrame, candle_min_window=3):
        originals_columns = list(ohlv.columns.copy())
        originals_columns.append("top")
        originals_columns.append("bottom")
        ohlv_temp = ohlv.copy()
        ohlv_temp["bottom"] = 1
        ohlv_temp["top"] = 1

        for window in range(1, candle_min_window + 1, 1):
            #create columns "_shifted[n_shift]" by using _shift_data_in_columns
            ohlv_temp = _shift_data_in_columns(ohlv_temp, ['close'], window)
            ohlv_temp = _shift_data_in_columns(ohlv_temp, ['close'], -window)

            # Calcul des noms des colonnes décalées
            n_minus_shifted_col = "close_shifted["+str(window)+"]"
            n_plus_shifted_col = "close_shifted["+str(-window)+"]"

            # Sélection des lignes où au moins l'une des valeurs décalées est inférieure/superieur à la valeur actuelle de "close"
            bottom_condition = (ohlv_temp[n_minus_shifted_col] < ohlv_temp["close"]) | (ohlv_temp[n_plus_shifted_col] < ohlv_temp["close"])
            up_condition = (ohlv_temp[n_minus_shifted_col] > ohlv_temp["close"]) | (ohlv_temp[n_plus_shifted_col] > ohlv_temp["close"])

            # Mise à jour de la colonne "bottom" pour ces lignes
            ohlv_temp.loc[bottom_condition, "bottom"] = 0
            ohlv_temp.loc[up_condition, "top"] = 0

        return ohlv_temp.loc[:,originals_columns]

    def _trend_get_level(self, ohlv:pd.DataFrame,candle_min_window=3, group_setting=3):
        #Detection top and bottom 
        ohlv_temp = self._trend_get_top_botom(ohlv=ohlv, candle_min_window=candle_min_window)

        #Create list top_bot points
        lst_bottom = ohlv_temp.loc[ohlv_temp["bottom"] == 1, "close"].tolist()
        lst_top = ohlv_temp.loc[ohlv_temp["top"] == 1, "close"].tolist()

        lst_top_bot = lst_bottom + lst_top
        lst_top_bot.sort()

        #Compute deltas between 2 consecutive points via zip
        deltas = [y - x for x, y in zip(*[iter(lst_top_bot)] * 2)]
        avg = sum(deltas) / len(deltas)

        important_levels = []
        current_level = [lst_top_bot[0]]  # Initialise la première sous-liste avec le premier élément de lst_top_bot

        for x in lst_top_bot[1:]:
            difference = x - current_level[0]
            if difference < group_setting * avg:
                current_level.append(x)  # Ajoute x à la sous-liste actuelle
            else:
                important_levels.append(current_level)  # Ajoute la sous-liste actuelle à important_levels
                current_level = [x]  # Commence une nouvelle sous-liste avec x comme premier élément

        # Ajoute la dernière sous-liste à important_levels
        important_levels.append(current_level)
        return important_levels


    def _trend_double_top_bottom(self, ohlv:pd.DataFrame,candle_min_window=2, top_bottom_reader=2, range_volatility=20, range_min_max=10, rate_min_max_local=0.1, rate_check_local=3, window_searching=20):
        #def extra columns vol / min / max + def list resultat + create unique
        ohlv["vol"] = abs(ohlv["close"] - ohlv["open"]).rolling(range_volatility).mean()
        ohlv["min_close"] = ohlv["close"].rolling(range_min_max).min()
        ohlv["max_close"] = ohlv["close"].rolling(range_min_max).max()
        ohlv['vol'] = ohlv['vol'].fillna(0)
        ohlv["sync"] = range(len(ohlv))
        tops_double_data = []
        test=[]
        bottoms_double_data = []

        #compute bot and top + select df top/bot only
        ohlv = self._trend_get_top_botom(ohlv=ohlv, candle_min_window=candle_min_window)
        ohlv_top_bottom = ohlv.loc[(ohlv["top"] == 1) | (ohlv["bottom"] == 1) & (ohlv["vol"] != 0)]

        #create list index + fin de boucle
        index_list = list(ohlv_top_bottom.index)
        len_df = len(ohlv_top_bottom)

        print("lol")

        #foreach bottom or top find max/min local + back on bottop 
        for ibottop in range(len_df - 2):
            # print(ibottop)
            if ohlv_top_bottom.iloc[ibottop]["top"] == 1 and ohlv_top_bottom.iloc[ibottop]["close"] >= ohlv_top_bottom.iloc[ibottop]["max_close"]:
                finder = ibottop
                min_local_find = False
                print(min_local_find)
                while ohlv_top_bottom.iloc[ibottop]["close"] > ohlv_top_bottom.iloc[finder]["close"] - rate_min_max_local * ohlv_top_bottom.iloc[finder]["vol"]:
                    
                    if finder < len_df - 1:
                        finder += 1
                    else: break #out of while if finder equal end 
                    # print("------------------------------")
                    # print(ohlv_top_bottom.iloc[ibottop]["close"] - ohlv_top_bottom.iloc[finder]["close"])
                    # print(rate_check_local * ohlv_top_bottom.iloc[ibottop]["vol"])
                    # print(ohlv_top_bottom.iloc[finder]["top"])
                    if ohlv_top_bottom.iloc[finder]["top"] != 1:
                        if ohlv_top_bottom.iloc[ibottop]["close"] - ohlv_top_bottom.iloc[finder]["close"] >= rate_check_local * ohlv_top_bottom.iloc[ibottop]["vol"]:
                            price_breakout = ohlv_top_bottom.iloc[finder]["close"]
                            index_breakout = ohlv.iloc[int(ohlv_top_bottom.iloc[finder]["sync"])]
                            min_local_find = True
                            print("<3")
                        continue

                    if  min_local_find and (abs(ohlv_top_bottom.iloc[ibottop]["close"] - ohlv_top_bottom.iloc[finder]["close"]) < rate_min_max_local * ohlv_top_bottom.iloc[finder]["vol"]):
                        print("Double Top:",index_list[ibottop],"->",index_list[finder])
                        iloc_sync_1st_tops = int(ohlv_top_bottom.iloc[ibottop]["sync"])
                        iloc_sync_2nd_tops = int(ohlv_top_bottom.iloc[finder]["sync"])

                        test.append(ohlv.iloc[iloc_sync_1st_tops].name)
                        tops_double_data.append({
                            "1st top": ohlv.iloc[iloc_sync_1st_tops].name,
                            "breakout": index_breakout.values[0],
                            "2nd top": ohlv.iloc[iloc_sync_1st_tops].values[0],
                            "breakout price" : price_breakout,
                            "confirmer_x1_reader" : price_breakout > ohlv.iloc[iloc_sync_2nd_tops + top_bottom_reader]["close"],
                            "price_x1_reader": ohlv.iloc[iloc_sync_2nd_tops + top_bottom_reader]["close"],
                            "confirmer_x2_reader" : price_breakout > ohlv.iloc[iloc_sync_2nd_tops + (top_bottom_reader * 2)]["close"],
                            "price_x2_reader": ohlv.iloc[iloc_sync_2nd_tops + (top_bottom_reader * 2)]["close"],
                            "confirmer_x5_reader" : price_breakout > ohlv.iloc[iloc_sync_2nd_tops + (top_bottom_reader * 5)]["close"],
                            "price_x5_reader": ohlv.iloc[iloc_sync_2nd_tops + (top_bottom_reader * 5)]["close"],
                            "confirmer_x10_reader" : price_breakout > ohlv.iloc[iloc_sync_2nd_tops + (top_bottom_reader * 10)]["close"],
                            "price_x10_reader": ohlv.iloc[iloc_sync_2nd_tops + (top_bottom_reader * 10)]["close"]
                        })

                    if finder - ibottop > window_searching:
                        break
    
        return tops_double_data, ohlv_top_bottom, test
                        

            #             if plot:
            #                 plot_double_top(df, int(df_top_bottom.iloc[i]["iloc"]), int(df_top_bottom.iloc[j]["iloc"]), df_top_bottom.iloc[j]["vol"], top_bottom_reader, top_or_bottom=1, hours_multiplier=hours_multiplier)
            #                 x = input()
            #                 if x == "x":
            #                     raise Exception("Stopped")
            #                 else:
            #                     clear_output(wait=True)
            #                     break
            #             else: break
            # elif df_top_bottom.iloc[i]["bottom"] == 1 and df_top_bottom.iloc[i]["close"] <= df_top_bottom.iloc[i]["min_close"]:
            #     j = i
            #     top_pass = False
            #     while df_top_bottom.iloc[i]["close"] < df_top_bottom.iloc[j]["close"] + 0.1 * df_top_bottom.iloc[j]["vol"]:
            #         if j < len_df - 1:
            #             j += 1
            #         else: break
            #         if df_top_bottom.iloc[j]["bottom"] != 1:
            #             if df_top_bottom.iloc[j]["close"] - df_top_bottom.iloc[i]["close"] > 3*df_top_bottom.iloc[i]["vol"]:
            #                 top_pass = True
            #             continue
            #         if j - i > 20:
            #             break
            #         if (abs(df_top_bottom.iloc[i]["close"] - df_top_bottom.iloc[j]["close"]) < 0.1 * df_top_bottom.iloc[j]["vol"]) and top_pass:
            #             # print("Double Bottom:",index_list[i],"->",index_list[j])
            #             iloc_j = int(df_top_bottom.iloc[j]["iloc"])
            #             price_detected = df.iloc[iloc_j+top_bottom_reader]["close"]
                        
            #             bottom_data.append({
            #                 "top1": index_list[i],
            #                 "top2": index_list[j],
            #                 "price_when_detected": price_detected,
            #                 "diff_mean_close_3": (df.iloc[iloc_j+top_bottom_reader+1:iloc_j+top_bottom_reader+4]["close"].mean() - price_detected) / price_detected,
            #                 "diff_mean_close_5": (df.iloc[iloc_j+top_bottom_reader+1:iloc_j+top_bottom_reader+6]["close"].mean() - price_detected) / price_detected,
            #                 "diff_mean_close_10": (df.iloc[iloc_j+top_bottom_reader+1:iloc_j+top_bottom_reader+11]["close"].mean() - price_detected) / price_detected,
            #                 "diff_mean_close_20": (df.iloc[iloc_j+top_bottom_reader+1:iloc_j+top_bottom_reader+21]["close"].mean() - price_detected) / price_detected,
            #                 "diff_mean_close_40": (df.iloc[iloc_j+top_bottom_reader+1:iloc_j+top_bottom_reader+41]["close"].mean() - price_detected) / price_detected,
            #             })
            #             if plot:
            #                 plot_double_top(df, int(df_top_bottom.iloc[i]["iloc"]), int(df_top_bottom.iloc[j]["iloc"]), df_top_bottom.iloc[j]["vol"], top_bottom_reader, top_or_bottom=0, hours_multiplier=hours_multiplier)
            #                 x = input()
            #                 if x == "x":
            #                     raise Exception("Stopped")
            #                 else:
            #                     clear_output(wait=True)
            #                     break
            #             else: break




    # def record_top_data(self,ohlv, ohlv_top_bottom, top_data, index_list, i, j, top_bottom_reader):
    #     iloc_j = int(ohlv_top_bottom.iloc[j]["unique"])
    #     price_detected = ohlv.iloc[iloc_j + top_bottom_reader]["unique"]
    #     top_data.append({
    #         "top1": index_list[i],
    #         "top2": index_list[j],
    #         "price_when_detected": price_detected,
    #         "diff_mean_close_3": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 3),
    #         "diff_mean_close_5": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 5),
    #         "diff_mean_close_10": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 10),
    #         "diff_mean_close_20": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 20),
    #         "diff_mean_close_40": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 40),
    #     })


                                                                                        # def _trend_get_double_top_bottom(self, ohlv:pd.DataFrame, top_bottom_reader=2):
                                                                                        #     # Calcul des indicateurs de volatilité
                                                                                        #     ohlv["volatility"] = abs(ohlv["close"] - ohlv["open"]).rolling(20).mean()
                                                                                        #     ohlv["min_close"] = ohlv["close"].rolling(10).min()
                                                                                        #     ohlv["max_close"] = ohlv["close"].rolling(10).max()
                                                                                        #     ohlv["unique"] = range(len(ohlv))
                                                                                            
                                                                                        #     # Initialisation des listes de données
                                                                                        #     double_top = []
                                                                                        #     double_bottom = []

                                                                                        #     # Marquage des tops et bottoms dans le DataFrame
                                                                                        #     ohlv = self._trend_get_top_botom(ohlv=ohlv, candle_min_window=top_bottom_reader)
                                                                                        #     ohlv_top_bottom = ohlv.loc[(ohlv["top"] == 1) | (ohlv["bottom"] == 1)]
                                                                                        #     index_list = list(ohlv_top_bottom.index)
                                                                                        #     len_ohlv_top_bottom = len(ohlv_top_bottom)
                                                                                            
                                                                                        #     # Recherche des doubles sommets et creux
                                                                                        #     for top_bottom in range(len_ohlv_top_bottom - 2):
                                                                                        #         if ohlv_top_bottom.iloc[top_bottom]["top"] == 1 and ohlv_top_bottom.iloc[top_bottom]["close"] >= ohlv_top_bottom.iloc[top_bottom]["max_close"]:
                                                                                        #             finder = top_bottom
                                                                                        #             bottom_pass = False
                                                                                        #             while ohlv_top_bottom.iloc[top_bottom]["close"] > ohlv_top_bottom.iloc[finder]["close"] - 0.1 * ohlv_top_bottom.iloc[finder]["volatility"]:
                                                                                        #                 finder += 1
                                                                                        #                 if finder == len_ohlv_top_bottom - 1:
                                                                                        #                     break
                                                                                        #                 if ohlv_top_bottom.iloc[finder]["top"] != 1:
                                                                                        #                     if ohlv_top_bottom.iloc[top_bottom]["close"] - ohlv_top_bottom.iloc[finder]["close"] > 3 * ohlv_top_bottom.iloc[top_bottom]["volatility"]:
                                                                                        #                         bottom_pass = True
                                                                                        #                     continue
                                                                                        #                 if finder - top_bottom > 20:
                                                                                        #                     break
                                                                                        #                 if abs(ohlv_top_bottom.iloc[top_bottom]["close"] - ohlv_top_bottom.iloc[finder]["close"]) < 0.1 * ohlv_top_bottom.iloc[finder]["volatility"] and bottom_pass:
                                                                                        #                     double_bottom.append(top_bottom)
                                                                                        #                     double_bottom.append(finder)

                                                                                        #         elif ohlv_top_bottom.iloc[top_bottom]["bottom"] == 1 and ohlv_top_bottom.iloc[top_bottom]["close"] <= ohlv_top_bottom.iloc[top_bottom]["min_close"]:
                                                                                        #             finder = top_bottom
                                                                                        #             top_pass = False
                                                                                        #             while ohlv_top_bottom.iloc[top_bottom]["close"] < ohlv_top_bottom.iloc[finder]["close"] + 0.1 * ohlv_top_bottom.iloc[finder]["volatility"]:
                                                                                        #                 finder += 1
                                                                                        #                 if finder == len_ohlv_top_bottom - 1:
                                                                                        #                     break
                                                                                        #                 if ohlv_top_bottom.iloc[finder]["bottom"] != 1:
                                                                                        #                     if ohlv_top_bottom.iloc[finder]["close"] - ohlv_top_bottom.iloc[top_bottom]["close"] > 3 * ohlv_top_bottom.iloc[top_bottom]["volatility"]:
                                                                                        #                         top_pass = True
                                                                                        #                     continue
                                                                                        #                 if finder - top_bottom > 20:
                                                                                        #                     break
                                                                                        #                 if abs(ohlv_top_bottom.iloc[top_bottom]["close"] - ohlv_top_bottom.iloc[finder]["close"]) < 0.1 * ohlv_top_bottom.iloc[finder]["volatility"] and top_pass:
                                                                                        #                     double_top.append(top_bottom)
                                                                                        #                     double_top.append(finder)
                                                                                            
                                                                                        #     return double_top, double_bottom, ohlv_top_bottom, ohlv
                                                                                        #     # # Affichage des résultats
                                                                                        #     # display_results(top_data, bottom_data)

    # def record_top_data(df, top_data, df_top_bottom, index_list, i, j, top_bottom_reader):
    #     iloc_j = int(df_top_bottom.iloc[j]["iloc"])
    #     price_detected = df.iloc[iloc_j + top_bottom_reader]["close"]
    #     top_data.append({
    #         "top1": index_list[i],
    #         "top2": index_list[j],
    #         "price_when_detected": price_detected,
    #         "diff_mean_close_3": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 3),
    #         "diff_mean_close_5": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 5),
    #         "diff_mean_close_10": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 10),
    #         "diff_mean_close_20": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 20),
    #         "diff_mean_close_40": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 40),
    #     })


    # def record_bottom_data(df, bottom_data, df_top_bottom, index_list, i, j, top_bottom_reader, plot, hours_multiplier):
    #     iloc_j = int(df_top_bottom.iloc[j]["iloc"])
    #     price_detected = df.iloc[iloc_j + top_bottom_reader]["close"]
    #     bottom_data.append({
    #         "top1": index_list[i],
    #         "top2": index_list[j],
    #         "price_when_detected": price_detected,
    #         "diff_mean_close_3": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 3),
    #         "diff_mean_close_5": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 5),
    #         "diff_mean_close_10": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 10),
    #         "diff_mean_close_20": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 20),
    #         "diff_mean_close_40": compute_mean_close_diff(df, iloc_j, top_bottom_reader, 40),
    #     })
    #     if plot:
    #         plot_double_top(df, int(df_top_bottom.iloc[i]["iloc"]), iloc_j, df_top_bottom.iloc[j]["vol"], top_bottom_reader, top_or_bottom=0, hours_multiplier=hours_multiplier)

    # def compute_mean_close_diff(df, iloc_j, top_bottom_reader, periods):
    #     return (df.iloc[iloc_j + top_bottom_reader + 1: iloc_j + top_bottom_reader + periods + 1]["close"].mean() - df.iloc[iloc_j + top_bottom_reader]["close"]) / df.iloc[iloc_j + top_bottom_reader]["close"]

    # def display_results(top_data, bottom_data):
    #     # Affichage des résultats pour les doubles sommets
    #     print("---", len(top_data), "Double Top détectés ---")
    #     display_mean_close_diff(top_data)
    #     # Affichage des résultats pour les doubles creux
    #     print("---", len(bottom_data), "Double Bottom détectés ---")
    #     display_mean_close_diff(bottom_data)

    # def display_mean_close_diff(data):
    #     for period in [3, 5, 10, 20, 40]:
    #         mean_diff = sum(d["diff_mean_close_" + str(period)] for d in data) / len(data)
    #         win_rate = 100 * len([d for d in data if d["diff_mean_close_" + str(period)] < 0]) / len(data)
    #         print(f"Évolution moyenne après {period} périodes : {mean_diff * 100:.2f}% | Taux de réussite : {win_rate:.2f}%")