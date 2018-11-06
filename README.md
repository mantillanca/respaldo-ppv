
# respaldo-ppv

## Para iniciar modificaciones de un repositorio existente 

```
> git init

> git clone https://github.com/mantillanca/respaldo-ppv.git

> git remote add PPV https://github.com/mantillanca/respaldo-ppv.git

``` 
Una vez hecho los cambios, se deben ejecutar las siguientes sentencias:

``` 
> git add .

> git commit -m "Venta, Usos, RNU, UNR"

> git push -u PPV master
```

## Conflictos en el push

Si existe algun conflicto en la carga de algunos de los archivos es preciso seguir los siguientes paso:

1.- Tal como muestra el ejemplo de acontinuación, para identificar las diferencias entre cambios se debe ejecutar la siguiente sentencia:
```
> git diff                                                                                         

diff --cc README.md
index f1a9af1,def74b5..0000000
--- a/README.md
+++ b/README.md
@@@ -1,12 -1,14 +1,14 @@@

...
```

2.- A continuación se deben modificar las diferencias y dejar setiada la version definitiva. Una manera de hacerlo es con el comando nano como se ve a continuación:
```
> nano README.md 
```

3.- Luego se deben incorporar los cambio con la siguiente sentencia
```
> git add .
```

4.- Agregar un comentario de los cambios
```
> git commit -m "Otro comentario"  
```

5.- Hacer efectivos los cambios
```
> git push PPV master
```
