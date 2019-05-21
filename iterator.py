#!/usr/bin/env python3


'''
This module provides two iterators over the Visual Genome data.

The remote iterator works over http.
The local iterator caches about 2.3GB of metadata,
but image pixel data and scene graphs are still retrieved via http.  # TODO: why?
'''


from abc import abstractmethod
from io import BytesIO
from os.path import isfile, isdir
from PIL import Image
import urllib
from visual_genome import api as vgr
from visual_genome import local as vgl
from zipfile import ZipFile


__all__ = ['Remote Local']  # TODO: this doesn't work


class Iter:
    '''
    ids: a list of ints between 1 and 108077. None means all.
    returns: (image metadata, region descriptions, scene graph).  # TODO: get a list of region graphs instead!
    '''
    def __init__(me, ids=None):
        super().__init__(ids)

    @abstractmethod
    def __iter__(me):
        pass


class Remote(Iter):
    def __init__(me, ids):
        if ids is None:
            ids = vgr.get_all_image_ids()
        me.ids = ids


    def __iter__(me):
        for id in me.ids:
            image = vgr.get_image_data(id)
            print('before', id)
            regions = vgr.get_region_descriptions_of_image(id)
            print('after')
            graph = vgr.get_scene_graph(id)
            yield image, regions, graph


class Local(Iter):
    '''data_dir: Gets created and overwritten with 2.3GB of cached data.'''
    def __init__(me, ids, data_dir='./data/'):
        me.data_dir = data_dir
        me.download_dataset(me.data_dir)

        images = vgl.get_all_image_data(data_dir)
        all_regions = vgl.get_all_region_descriptions(data_dir)  # slow
        if ids is None:
            ids = list(range(1, len(images)+1))
        me.ids = ids


    def __iter__(me):
        for id in me.ids:
            image = images[id-1]
            regions = all_regions[id-1]
            graph = vgl.get_scene_graph(id
                                       ,images=me.data_dir
                                       ,image_data_dir=me.data_dir+'/by-id/'
                                       ,synset_file=me.data_dir+'/synsets.json')
            yield image, regions, graph


    @staticmethod
    def download_dataset(path):
        def download_zip(path, url):
            with urllib.request.urlopen(url) as response:
                stream = BytesIO(response.read())
                zip = ZipFile(stream)
                zip.extractall(path)

        def get(resource):
            vg_url = 'http://visualgenome.org/static/data/dataset'
            resource = '/' + resource + '.json'
            if not isfile(path+resource):
                print('Downloading', vg_url+resource+'.zip' )
                download_zip(path, vg_url+resource+'.zip')

        get('image_data')
        get('region_descriptions')
        get('scene_graphs')
        if not isdir(path+'/by-id'):
            vgl.save_scene_graphs_by_id(data_dir=path, image_data_dir=path+'/by-id/')
        get('synsets')


def main():
    from random import randint
    ids = [randint(1, 108077) for i in range(20)]
    remote = Remote(ids)
    local = Local(ids)
    for r, l  in zip(remote, local):
        assert r == l


if __name__ == '__main__':
    main()
