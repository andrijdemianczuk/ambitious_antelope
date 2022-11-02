class CommitDelta:
  def __init__(self):
    pass
  
  def init_props(type) -> dict:
    return {
      "delta_loc": f"dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/{type}",
      "write_fmt": 'delta',
      "table_name": f'b_carrier_{type}',
      "write_mode": 'overwrite',
      "type": type,
      "database": "ademianczuk"
    }

  def update_delta_fs(df, params):
    df.write \
    .format(params["write_fmt"]) \
    .mode(params["write_mode"]) \
    .save(params["delta_loc"])

  def create_delta_table(df, params):

    type = params["type"]
    database = params["database"]
    table_name = params["table_name"]
    delta_loc = params["delta_loc"]

    if spark._jsparkSession.catalog().tableExists(database, table_name):

      print("table already exists..... appending data")
      df.createOrReplaceTempView(f"vw_{type}")  

      spark.sql(f"MERGE INTO {database}.{table_name} USING vw_{type} \
        ON vw_{type}.h3index = {database}.{table_name}.h3index \
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

    else:
      print("table does not exist..... creating a new one")

      # Write the data to its target.
      spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc + "'")
 