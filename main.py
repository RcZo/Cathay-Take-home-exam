import pandas as pd
import cn2an as c2n

if __name__ == '__main__':
  # read file
  df_a=pd.read_csv("./data/a_lvr_land_a.csv")
  df_b=pd.read_csv("./data/b_lvr_land_a.csv")
  df_e=pd.read_csv("./data/e_lvr_land_a.csv")
  df_f=pd.read_csv("./data/f_lvr_land_a.csv")
  df_h=pd.read_csv("./data/h_lvr_land_a.csv")

  # union
  print(df_a.shape, df_b.shape, df_e.shape, df_f.shape, df_h.shape)
  df_all=pd.concat([df_a, df_b, df_e, df_f, df_h], ignore_index=True)
  print(df_all.shape)
  df_all = df_all[(df_all.總樓層數 != "total floor number")]  # remove useless
  print(df_all.head(5))

  # filter_a
  filter_a=df_all[["主要用途","建物型態","總樓層數"]] #select columns  
  filter_a=filter_a[(filter_a.總樓層數.notnull())] #remove nan
  filter_a["總樓層數"] = filter_a["總樓層數"].str.rstrip("層") #replace word
  filter_a["總樓層數"]=[c2n.cn2an(x) for x in filter_a["總樓層數"]] #convert cn2an
  filter_a=filter_a[(filter_a["主要用途"]=="住家用")&(filter_a["建物型態"].str.contains("住宅大樓"))&(filter_a["總樓層數"]>=13)] #final
  print(filter_a.head(5))

  filter_a.to_csv("./export/filter_a.csv",index=True)

  # filter_b
  filter_b=df_all[["交易筆棟數","總價元","車位總價元"]] #select columns
  filter_b=filter_b[(filter_b.交易筆棟數.notnull())] #remove nan
  filter_b["車位數"] = filter_b["交易筆棟數"].str.split("車位").str[1] #split word
  print(filter_b.head(5))
  total_num=len(filter_b) #總件數
  total_parking=filter_b["車位數"].astype(int).sum() #總車位數
  avg_price=round(filter_b["總價元"].astype(float).mean(), 6).astype(str) #平均總價元
  avg_price_pakin=round(filter_b["車位總價元"].astype(float).mean(), 6).astype(str) #平均車位總價元
  final_df = pd.DataFrame({"總件數": [total_num], "總車位數": [total_parking], "平均總價元": [avg_price], "平均車位總價元": [avg_price_pakin]})
  print(final_df)

  final_df.to_csv("./export/filter_b.csv",index=True)



