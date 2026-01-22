# ================================================================
# 1. Desde la raiz del proyecto: Construir la imagen del backend
# ================================================================
```bash
cd code
sudo docker build -t backend .
```

# ================================================================
# 2. Desde la raiz del proyecto: Construir la imagen del frontend
# ================================================================
```bash
cd ui
sudo docker build -t frontend .
```

# ================================================================
# 3. Obtener IP de la maquina propia
# ================================================================
```bash
ip a
```
Sale 1.lo esa es de loopback, es la 2 (se busca en el reguero de la 2 lo que diga algo como 192.168.45.212/24, se copia sin la mascara /24 o la que sea)

# ================================================================
# 4. Iniciar swarm
# ================================================================
```bash
sudo docker swarm init --advertise-addr <IP DE LA MAQUINA PROPIA, ej: 192.168.45.212>
```
# ================================================================
# 5. Crear la red overlay
# ================================================================

 ```bash
 sudo docker network create --driver overlay --attachable teamnet
 ```

 Para ver la red si se creo:
 ```bash
sudo docker network ls
 ```
 
# ================================================================
# 5. Montar nodos backend
# ================================================================
```bash
sudo docker run --rm -it --name backend_A_1 --network teamnet --network-alias spotify_cluster backend
```

--rm es para no tener que borrarlo, si lo quitas se borra solo.
-v es para si haces cambios en el codigo se reflejen sin tener que quitar el contenedor y volverlo a montar
-it es para que se ejecute en la consola actual

Sin usar -it se crea el contenedor y puedes ejecutarlo en otra consola con:
```bash
sudo docker logs -f backend_node_1
```

# ================================================================
# 5. Montar nodos frontend
# ================================================================
```bash
sudo docker run --rm -it --name frontend --network teamnet -p 8080:8080 -p 3000:3000 -v .:/app frontend
```

Lo mismo si no usas -it puedes verlo ejecutando con
```bash
sudo docker logs -f frontend
```

# ================================================================
# 6. Ver si hay cosas creadas en base de datos
# ================================================================
Ejemplo en backend_node_1, en una consola cualquiera haces:
```bash
sudo docker exec -it backend_node_1 python manage.py  shell
```

Con eso abres la consola de python django.

Luego ahí haces:

```bash
from app.models import *
```

Y usas los modelos para hacer peticiones normal, las mas faciles es por cada modelo pedir todo, otras se las pides a chatgpt:
```bash
app.models.Track.objects.all() o directamente Track.objects.all()
```

muestra todas las canciones. Cambiar Track por Artist o Album para ver esos.

# ================================================================
# 7. Ver si hay shards copiados
# ================================================================
Ejemplo para ver los archivos del nodo backend_node_1, desde una consola cualquiera:
```bash
sudo docker exec -it backend_node_1 /bin/bash

o /bin/sh
```

De ahi es usar ls y cd para navegar y ver si tiene los shards creados