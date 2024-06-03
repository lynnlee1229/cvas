# Image extractor using 3 different methods

1. Tile and join with MBR. The output images are set to be customized resolution, whose default is 256*256.
   To test this method, use CLI option raptorresult:tile. e.g.

```shell
beast imagex HYP_LR.tif NE_states_provinces.geojson ./imagex raptorresult:tile
```

2. Tile and repartitionAndSort and Iterator. The output images are the original resolution.
   To test this method, use CLI option raptorresult:tileItr. e.g.

```shell
beast imagex HYP_LR.tif NE_states_provinces.geojson ./imagex raptorresult:tileItr
```

3. Pixel and join with MBR. The output images are set to be customized resolution, whose default is 256*256.
   To test this method, use CLI option raptorresult:pixel. e.g.

```shell
beast imagex HYP_LR.tif NE_states_provinces.geojson ./imagex raptorresult:pixel
```