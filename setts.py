# -*- coding: utf-8 -*-

import numpy

GEN_COUNT = 1000

DIGITS = "0123456789"
LETTERS = "ABEIKMHOPCTX"

SPECIAL_SYMBOLS = " "
CHARS = DIGITS + LETTERS + SPECIAL_SYMBOLS


def softmax(a):
    exps = numpy.exp(a.astype(numpy.float64))
    return exps / numpy.sum(exps, axis=-1)[:, numpy.newaxis]


def sigmoid(a):
    return 1. / (1. + numpy.exp(-a))
