class UcExt:
#Contains a decorator for writing to Unity Catalog
  
  def addToUc(func):
    def ucWrapper(*args, **kwargs):
      print("first, select the catalog and prepend it as part of the UC namespacing requirements")
      funct(*args, **kwargs)
      print("last, write the the proper UC catalog")
    return ucWrapper