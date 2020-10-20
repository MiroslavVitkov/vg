#!/usr/bin/env python3


'''
This module provides two iterators over the Visual Genome dataset.

The remote iterator works over http.
The local iterator caches about 2.3GB of metadata,
but pixel data and scene graphs are still retrieved via http.
Because of their large size, the underlying library doesn't support
caching them.
'''


from io import BytesIO
from os.path import isfile, isdir
import pickle
from PIL import Image
import urllib
from visual_genome import api as vgr
from visual_genome import local as vgl
from visual_genome.models import Image as ImgHeader
from visual_genome.models import Region
from zipfile import ZipFile


# Picture identifier.
ID = int


class Iter:
    ERR = NotImplementedError('This is an abstract base class.'
                              'Please use one of the derived classes instead.')


    def __init__(me, ids: [ID]=None):
        '''ids: A list of image ids. None means all.'''
        raise ERR


    @staticmethod
    def sample(n: int):
        '''Alternative c-tor. Iterate over n random images.'''
        raise ERR


    def get_all_image_ids(me) -> [ID]:
        '''returns: the ids of all images in the visual genome.'''
        raise ERR


    def __iter__(me) -> (ImgHeader, Region, None):  # TODO!
        '''returns: [tuple] (image metadata, regions in image, scene graph).'''
        raise ERR


class Remote(Iter):
    def __init__(me, ids: [ID]):
        if ids is None:
            ids = me.get_all_image_ids()
        me.ids = ids


    @staticmethod
    def get_all_image_ids() -> [ID]:
        return vgr.get_all_image_ids()


    def __iter__(me):
        for id in me.ids:
            regions = vgr.get_region_descriptions_of_image(id)
            image = regions[0].image
            # graph = vgr.get_scene_graph_of_image(id)  # slow
            graph = None
            yield image, regions, graph


class Local(Iter):
    def __init__(me, ids: [ID], data_dir: str='./data/'):
        '''data_dir: Gets created and overwritten with 2.3GB of cached data.'''
        me.data_dir = data_dir
        me.download_dataset(me.data_dir)

        all_regions = vgl.get_all_region_descriptions(data_dir)  # slow
        # r here is the list of all regions in one image
        me.regions = {r[0].image.id: r
                      for r in all_regions
                      if r[0].image.id in ids}

        if ids is None:
            ids = me.get_all_image_ids()
        me.ids = ids


    def get_all_image_ids(me) -> [ID]:
        with open(me.data_dir+'/all_image_ids', 'rb') as f:
            return pickle.load(f)


    def __iter__(me):
        for id in me.ids:
            regions = me.regions[id]
            image = regions[0].image
#            graph = vgl.get_scene_graph(id
#                                       ,images=me.data_dir
#                                       ,image_data_dir=me.data_dir+'/by-id/'
#                                       ,synset_file=me.data_dir+'/synsets.json')
            graph = None
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
        if not isfile(path+'/all_image_ids'):
            ids = Remote.get_all_image_ids()
            with open(path+'/all_image_ids', 'wb') as f:
                pickle.dump(ids, f)


# TODO: split this horror into profile() and verify()
# TODO: figure out python's object comparrison rules
# perhaps do all(a == b for a, b in izip_longest(gen_1, gen_2, fillvalue=sentinel))
# or str(l) == str(r)
def main():
    from random import sample
    from time import time
    points = 1

    all_ids = Remote.get_all_image_ids()
    ids = sample(all_ids, points)

    remote = Remote(ids)
    local = Local(ids)

    start = time()
    rem = list(remote)
    print('Time to read one record remotely:', (time()-start)/points, 'seconds.')

    start = time()
    loc = list(local)
    print('Time to read one record locally:', (time()-start)/points, 'seconds.')

    for r, l  in zip(rem, loc):
        print('========================== r0 =============================')
        print(r[0])
        print('========================== l0 =============================')
        print(l[0])
        assert r[0].id == l[0].id
        assert r[0].coco_id == l[0].coco_id
        assert r[0].id == l[0].id
        assert r[0].id == l[0].id
#        assert r[1] == l[1]
#        assert r[2] == l[2]


if __name__ == '__main__':
    main()
