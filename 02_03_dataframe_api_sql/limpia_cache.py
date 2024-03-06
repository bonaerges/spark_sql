import gc

# Forzar la recolección de basura
gc.collect()

# Obtener todos los objetos rastreados por el GC
objects = gc.get_objects()
print(f"Total objects tracked by GC before cleanup: {len(objects)}")

# Eliminar las referencias a todos los objetos
for obj in objects:
    del obj

# Forzar la recolección de basura nuevamente
gc.collect()

# Comprobar cuántos objetos quedan
objects = gc.get_objects()
print(f"Total objects tracked by GC after cleanup: {len(objects)}")