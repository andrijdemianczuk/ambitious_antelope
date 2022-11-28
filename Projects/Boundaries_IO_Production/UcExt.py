#Contains a decorator(s) for writing to Unity Catalog
class UcExt:
#   import functools #tools for proper introspection
  
#   def __init__(self, func):
#     this.func = func
  
#   decorator template
#   @functools.wraps(func)
  def decorator(func):
    def wrapper_decorator(*args, **kwargs):
      #Do something before
      print("First")
      value = func(*args, **kwargs)
      #Do something after
      print("last")
      return value
    return wrapper_decorator