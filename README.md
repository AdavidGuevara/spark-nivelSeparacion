# Nivel de separacion de heroes.

- Este proyecto es una mejora a la solucion propuesta en https://github.com/urcuqui/Apache-Spark/blob/master/python/degrees-of-separation.py, incorporando variables broadcast y metodos para las distintas versiones de un mismo heroe.
- Este programa se desarrollo utilizando la siguiente imagen de docker y el respectivo comando: docker run -it --rm -p 10000:8888 -v "${PWD}":/home/jovyan/work jupyter/pyspark-notebook. Aunque tambien se incluye un fichero .py para poder ejecutarlo en un cluster local.