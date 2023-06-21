from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("gradosDeSeparacion")
sc = SparkContext.getOrCreate(conf=conf)

# Definimos los heroes que deseamos analizar:
startHeroId = 5306
targetHeroID = 14

# Preparamos el documento a analizar:
file = sc.textFile("file:///data/Marvel-graph.txt")

# Iniciamos el acumulador en cero:
hitCounter = sc.accumulator(0)


# Funcion para convetir las lineas del rrd en nodos bfs:
def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    # Por defecto todos los nodos seran de color blanco y su distancia "infinita",
    # La unica exepcion es precisamente el nodo padre.
    color = "WHITE"
    distance = 9999

    if heroID == startHeroId:
        color = "GRAY"
        distance = 0

    return (heroID, (connections, distance, color))


# Pueden haber varias versiones de un heroe, se concatenara la lista de amigos:
def reduceRdd(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    edges = edges1 + edges2
    distance = 9999
    color = "WHITE"

    if distance1 == 0:
        distance = distance1

    if distance2 == 0:
        distance = distance2

    # Preserve darkest color
    if color1 == "GRAY":
        color = color1

    if color2 == "GRAY":
        color = color2

    return (edges, distance, color)


# Abrimos el archivo y extraemos las variables broadcast.
# Se creara una diccionario con el id del hero como clave y
# el valor asociado sera la lista de amigos:
def loadHeroConns():
    heroConns = {}
    heroList = []
    with open("/home/jovyan/work/data/Marvel-graph.txt") as file:
        for line in file:
            fields = line.split()
            heroID = int(fields[0])
            connections = []
            for connection in fields[1:]:
                connections.append(int(connection))

            # Pueden haber varias versiones de un heroe, por lo que vamos a concatenar la lista de amigos.
            if heroID not in heroList:
                heroList.append(heroID)
                heroConns[heroID] = connections
            else:
                heroConns[heroID] += connections

    return heroConns


# Cargamos la variable broadcast:
heroConns = sc.broadcast(loadHeroConns())


# Funcion que analiza el arbol de nodos que se va generando desde el nodo padre:
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # Si el color es gris, entonces o es el nodo padre o un nodo hijo que esta siendo analizado:
    if color == "GRAY":
        for hero in connections:
            newConn = heroConns.value[int(hero)]
            newDistance = distance + 1
            newColor = "GRAY"
            if targetHeroID == hero:
                # Guarda el numero de direcciones en donde hay una relacion entre los heroes:
                hitCounter.add(1)
            results.append((hero, (newConn, newDistance, newColor)))

        # Cuando el nodo ya fue analizado su color pasa a ser negro:
        color = "BLACK"

    # Se guarda el nodo analizado:
    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1, data2):
    edges = data1[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]
    # Gracias a que agrupamos las distintas versiones de un heroe, entonces si un heroe
    # aparece mas de una ves en la lista de resultados, la lista de sus amigos debe ser la misma.

    distance = 9999
    color = color1

    # Se preserva la minima distancia
    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    # Se preserva el color mas oscuro, lo cual puede indicar que ese heroe ya fue analizado:
    if color1 == "WHITE" and (color2 == "GRAY" or color2 == "BLACK"):
        color = color2

    if color1 == "GRAY" and color2 == "BLACK":
        color = color2

    if color2 == "WHITE" and (color1 == "GRAY" or color1 == "BLACK"):
        color = color1

    if color2 == "GRAY" and color1 == "BLACK":
        color = color1

    return (edges, distance, color)


# Creamos el rdd con todos los nodos de los heroes y posteriormente se reduce:
rdd = file.map(convertToBFS)
iterationRdd = rdd.reduceByKey(reduceRdd)

# Hacemos las iteraciones:
maxIter = 25
for iteration in range(0, maxIter):
    # Creacion del arbol de nodos:
    mapped = iterationRdd.flatMap(bfsMap)
    print(f"Iteracion {iteration + 1}: Procesando {mapped.count()} elementos.")

    if hitCounter.value > 0:
        print(f"\nDistancia entre los heroes: {iteration}.")
        break

    if hitCounter.value == 0 and iteration == maxIter - 1:
        print("Los heroes escogidos no tienen ningun tipo de contacto.")

    # Reducir los datos por heroe, presevando los que ya fueron analisados:
    iterationRdd = mapped.reduceByKey(bfsReduce)
