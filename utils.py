from datetime import datetime
import json
from QuantLib import *
import numpy as Numpy
import boto3
import os
import csv

def clear_output_bucket():
    botoSession = boto3.session.Session(region_name='us-east-1')
    s3 = botoSession.resource('s3')
    bucketname = os.environ['BUCKET_OUT']
    bucket = s3.Bucket(bucketname)
    objects = bucket.objects.all()
    objects.delete()

def ql_to_pydate(d):
    return datetime(d.year(), d.month(), d.dayOfMonth())

def str_to_pydate(s):
    return datetime.strptime(s, '%m/%d/%Y')
    
def py_to_qldate(d):
    return Date(d.day, d.month,d.year)
    
# Builds a QuantLib swap object from given specification
def makeSwap(today, start, maturity, nominal, fixedRate, index, typ=VanillaSwap.Payer):
    calendar = UnitedStates()
    fixedLegTenor = Period(6, ql.Months)
    floatingLegBDC = ModifiedFollowing
    fixedLegDC = Thirty360(ql.Thirty360.BondBasis)
    spread = 0.0
    settle_date = calendar.advance(start, 2, ql.Days)
    end = calendar.advance(settle_date, maturity, floatingLegBDC)

    fixedSchedule = ql.Schedule(settle_date,
                                end,
                                fixedLegTenor,
                                calendar,
                                ql.ModifiedFollowing, ql.ModifiedFollowing,
                                ql.DateGeneration.Forward, False)
    floatSchedule = ql.Schedule(settle_date,
                                end,
                                index.tenor(),
                                index.fixingCalendar(),
                                index.businessDayConvention(),
                                index.businessDayConvention(),
                                ql.DateGeneration.Forward,
                                False)
    swap = ql.VanillaSwap(typ,
                          nominal,
                          fixedSchedule,
                          fixedRate,
                          fixedLegDC,
                          floatSchedule,
                          index,
                          spread,
                          index.dayCounter())
    #return swap, [index.fixingDate(x) for x in floatSchedule if index.fixingDate(x) >= today][:-1]
    return swap
    
# class for hosting simulation grid (dates, times)
class Grid:
    def __init__(self, startDate, endDate, tenor):
        # create date schedule, ignore conventions and calendars
        self.schedule = Schedule(startDate, endDate, tenor, NullCalendar(),
                                 Unadjusted, Unadjusted, DateGeneration.Forward, False)
        self.dayCounter = Actual365Fixed()
        self.tenor = tenor

    def GetDates(self):
        # get list of scheduled dates
        dates = []
        [dates.append(self.schedule[i]) for i in range(self.GetSize())]
        return dates

    def GetTimes(self):
        # get list of scheduled times
        times = []
        [times.append(self.dayCounter.yearFraction(self.schedule[0], self.schedule[i]))
         for i in range(self.GetSize())]
        return times

    def GetMaturity(self):
        # get maturity in time units
        return self.dayCounter.yearFraction(self.schedule[0], self.schedule[self.GetSteps()])

    def GetSteps(self):
        # get number of steps in schedule
        return self.GetSize() - 1

    def GetSize(self):
        # get total number of items in schedule
        return len(self.schedule)

    def GetTimeGrid(self):
        # get QuantLib TimeGrid object, constructed by using list of scheduled times
        return TimeGrid(self.GetTimes(), self.GetSize())

    def GetDt(self):
        # get constant time step
        return self.GetMaturity() / self.GetSteps()

    def GetTenor(self):
        # get grid tenor
        return self.tenor
        
# term A(t, T) for analytical Hull-White zero-coupon bond price
def A(curve, a, sigma, t, T):
    f = curve.forwardRate(t, t, Continuous, NoFrequency).rate()
    value = B(a, t, T) * f - 0.25 * sigma * B(a, t, T) * sigma * B(a, t, T) * B(a, 0.0, 2.0 * t);
    return exp(value) * curve.discount(T) / curve.discount(t);


# term B(t, T) for analytical Hull-White zero-coupon bond price
def B(a, t, T):
    return (1.0 - exp(-a * (T - t))) / a;
        
# class for hosting calibration helpers and calibration procedure for a given model
class ModelCalibrator:
    def __init__(self, endCriteria):
        self.endCriteria = endCriteria
        self.helpers = []

    def AddCalibrationHelper(self, helper):
        self.helpers.append(helper)

    def Calibrate(self, model, engine, curve, fixedParameters):
        # assign pricing engine to all calibration helpers
        for i in range(len(self.helpers)):
            self.helpers[i].setPricingEngine(engine)
        method = LevenbergMarquardt()
        if (len(fixedParameters) == 0):
            model.calibrate(self.helpers, method, self.endCriteria)
        else:
            model.calibrate(self.helpers, method, self.endCriteria,
                            NoConstraint(), [], fixedParameters)
                            
# load discount curve from S3
def load_discount_curve():
    s3 = boto3.client('s3')
    bucket = os.environ['BUCKET_IN']
    disc_curve_file = os.environ['DISC_CURVE_FILE']
    csvfile = s3.get_object(Bucket=bucket, Key=disc_curve_file)
    lines = csvfile['Body'].read().decode('utf-8').split()
    csv_data = {}
    for row in csv.DictReader(lines):
        csv_data[row['date']] = row['discount_factor']
    termStructureDates = [py_to_qldate(str_to_pydate(x)) for x in csv_data.keys()]
    termStructureDiscountFactors = [float(x) for x in csv_data.values()]
    # create yield term structure from a given set of discount factors
    yieldTermStructure = DiscountCurve(termStructureDates, termStructureDiscountFactors, Actual360(), TARGET())
    yieldTermStructure.enableExtrapolation()
    return yieldTermStructure
    
# load simulated fixings
def load_simulated_fixings():
    s3 = boto3.client('s3')
    bucket = os.environ['BUCKET_OUT']
    sim_fixings_file = os.environ['SIMULATED_FIXINGS_FILE']
    csvfile = s3.get_object(Bucket=bucket, Key=sim_fixings_file)
    lines = csvfile['Body'].read().decode('utf-8').split()
    fixingsLookupTable = {}
    for row in csv.DictReader(lines):
        fixingsLookupTable[py_to_qldate(str_to_pydate(row['date']))] = float(row['fixing'])
    return fixingsLookupTable
    
def CreateSwapTransaction(index):
    # create benchmarking IR receiver swap, PV(t = 0) = 0.0
    fixedSchedule = Schedule(Date(14, December, 2018), Date(14, December, 2023), Period(1, Years), TARGET(), \
                             ModifiedFollowing, ModifiedFollowing, DateGeneration.Backward, False)

    floatingSchedule = Schedule(Date(14, December, 2018), Date(14, December, 2023), Period(3, Months), TARGET(), \
                                ModifiedFollowing, ModifiedFollowing, DateGeneration.Backward, False)

    swap = VanillaSwap(VanillaSwap.Receiver, 10000000.0, fixedSchedule, 0.03, Actual365Fixed(), \
                       floatingSchedule, index, 0.001277206920730623, Actual360())
    return swap