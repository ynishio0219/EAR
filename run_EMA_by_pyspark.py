# import pandas as pd
# from pyspark.sql.functions import col, collect_list, udf, ArrayType, explode
# from pyspark.sql.types import DoubleType

# Define the UDF to calculate EMA
# def calculate_ema(values, alpha):
#     ema = []
#     for i, value in enumerate(values):
#         if i == 0:
#             ema.append(value)
#         else:
#             ema.append(alpha * value + (1 - alpha) * ema[i-1])
#     return ema

alpha = float(dbutils.widgets.get("alpha"))
# print(alpha)


from pyspark.sql.functions import udf, collect_list, col, explode 
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.window import Window




calculate_ema_udf = udf(lambda x: calculate_ema(x, alpha=alpha), ArrayType(DoubleType()))


# Reset index before applying the function

# IDごとに適用
# df['ema'] = df.groupby('mm_panelist_id', group_keys=False)['ans_ratio'].apply(lambda x: calculate_ema(x, alpha=0.1))


# df.display()

# Group by 'mm_panelist_id' and collect 'ans_ratio' into a list
df_grouped_panelist_id = df.groupBy('mm_panelist_id').agg(collect_list('ans_ratio').alias('ans_ratios')).orderBy('mm_panelist_id')

# Window 関数を定義（IDごとに日付で並び替え）
# window_spec = Window.partitionBy("mm_panelist_id").orderBy("closed_dt")
# collect_list を IDごとに適用（closed_dt を落とさない）
# df_grouped = df.withColumn("ans_ratios", collect_list("ans_ratio").over(window_spec))


# Apply the UDF to calculate EMA
df_with_ema = df_grouped_panelist_id.withColumn('ema', calculate_ema_udf(col('ans_ratios')))
df_with_ema = df_with_ema.withColumn('ema_last', F.expr("ema[SIZE(ema)-1]"))

# get the last closed date of each panelist
# df_desc_sorted = df.groupBy('mm_panelist_id').agg(F.max('closed_dt').alias('closed_dt'))
df_desc_sorted = df.groupBy('mm_panelist_id').agg(F.max('create_dt').alias('create_dt'))


# df_out = df_desc_sorted.join(df_with_ema, on=['mm_panelist_id'], how='inner').select('mm_panelist_id', 'closed_dt', 'ema_last')
df_out = df_desc_sorted.join(df_with_ema, on=['mm_panelist_id'], how='inner').select('mm_panelist_id', 'ema_last')
# # Explode the EMA list to get one row per EMA value
# df_exploded = df_with_ema.select('mm_panelist_id', explode(col('ema')).alias('ema'))

# Join back with the original DataFrame to get 'closed'
# df_out = df.join(df_exploded, on=['mm_panelist_id'], how='inner').select('mm_panelist_id', 'closed_dt', 'ema')
# df_out.display()

# ema の最新のものをとってくる. emaの計算の仕方を再度チェック
# csvで吐き出す．　sparkTOpandasで吐き出す volume が良い


# display(df_with_ema)