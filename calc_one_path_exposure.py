import json
from QuantLib import *
import numpy as Numpy
import boto3
import os
import csv
from utils import *

global_cache = {}

def lambda_handler(event, context):
    
    simulation_num = event['simulation_num']
    firstIndexFixing = event['first_index_fixing']
    a = event['a']
    sigma = event['sigma']
    settlementDate = py_to_qldate(str_to_pydate(event['settlement_date']))
    endDate = py_to_qldate(str_to_pydate(event['end_date']))
    gridStepPeriod = Period(event['grid_step_period'])
    onePath = Numpy.asarray(event['one_path'])
    
    if ('market_curve' in global_cache):
        marketCurve = global_cache['market_curve']
    else:
        marketCurve = load_discount_curve()
        global_cache['market_curve'] = marketCurve

    fixingsLookupTable = load_simulated_fixings()
    
    forecastingCurve = RelinkableYieldTermStructureHandle()
    index = USDLibor(Period(3, Months), forecastingCurve)
    transaction = CreateSwapTransaction(index)

    # link transaction and pricing engine
    discountingCurve = RelinkableYieldTermStructureHandle()
    swapEngine = DiscountingSwapEngine(discountingCurve)
    transaction.setPricingEngine(swapEngine)

    # request simulation grid, define times and dates
    grid = Grid(settlementDate, endDate, gridStepPeriod)
    times = Numpy.array(grid.GetTimes())
    dates = Numpy.array(grid.GetDates())
    
    # request transaction floating leg fixings dates
    scheduleDates = Numpy.array(list(transaction.floatingSchedule()))
    transactionFixingDates = Numpy.array([index.fixingDate(scheduleDates[i]) for i in range(scheduleDates.shape[0])])
    transactionFixingRates = Numpy.zeros(shape=(transactionFixingDates.shape[0]))

    # add transaction fixing rates for a given date from fixings lookup table
    for i in range(transactionFixingDates.shape[0]):
        if transactionFixingDates[i] in fixingsLookupTable:
            transactionFixingRates[i] = fixingsLookupTable[transactionFixingDates[i]]
        else:
            # find the nearest fixing from lookup table
            transactionFixingRates[i] = \
                fixingsLookupTable.get(transactionFixingDates[i], \
                                       fixingsLookupTable[min(fixingsLookupTable.keys(), \
                                                              key=lambda d: abs(d - transactionFixingDates[i]))])

    # add required transaction fixing dates and rates to floating leg index
    index.addFixings(transactionFixingDates, transactionFixingRates, True)
    
    curves = Numpy.zeros((grid.GetSize(),), dtype=DiscountCurve)
    
    exposures = Numpy.zeros((grid.GetSize(),), dtype=float)
    
    curves[0] = marketCurve
    
    # loop through time steps
    for t in range(1, grid.GetSteps()):
        curveDate = dates[t]
        gridTenor = grid.GetTenor()
        curveDates = [curveDate] + [curveDate + (gridTenor * k) for k in range(1, grid.GetSize())]
        rt = onePath[t]
        # define list for simulated zero-coupon bonds
        # set the first discount factor to be 1.0
        zeros = Numpy.zeros((grid.GetSize(),), dtype=float)
        zeros[0] = 1.0
        dt = grid.GetDt()
        for k in range(1, grid.GetSize()):
            # use analytic formula for Hull-White zero-coupon bond price
            A_term = A(marketCurve, a, sigma, times[t], times[t] + (dt * k))
            B_term = B(a, times[t], times[t] + (dt * k))
            zeros[k] = A_term * exp(-B_term * rt)

        # create a new curve from simulated zero-coupon bond prices
        curves[t] = DiscountCurve(curveDates, zeros, Actual360(), TARGET())
        curves[t].enableExtrapolation()
    
     
    for t in range(grid.GetSteps()):
        # move forward in time along the grid
        Settings.instance().evaluationDate = dates[t]
        curve = curves[t]
        discountingCurve.linkTo(curve)
        forecastingCurve.linkTo(curve)
        # save pv to exposure matrix
        exposures[t] = transaction.NPV()
    
    str_exposures = ','.join(exposures.astype(str)).encode('utf-8')
    s3_path = str(simulation_num) + '_' + 'exposures.csv'
    bucket = os.environ['BUCKET_OUT']
    s3 = boto3.resource("s3")
    s3.Bucket(bucket).put_object(Key=s3_path, Body=str_exposures)