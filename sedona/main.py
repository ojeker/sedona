from sedona.spark import SedonaContext
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter

import contextFactory

def main():
    p_bound = '/home/oliver/code/sedona/data/ch/boundaries/swissBOUNDARIES3D_1_5_TLM_HOHEITSGRENZE.shp'
    sx = contextFactory.fetch_for_name('highgem')
    gem_rdd = ShapefileReader.readToPolygonRDD(sx.sparkContext, p_bound)
    Adapter.toDf(gem_rdd, sx.sparkContext).printSchema()

