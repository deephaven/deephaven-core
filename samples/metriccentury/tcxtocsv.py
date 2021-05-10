# mostly due to this code:
# https://github.com/coreysiegel/tcx-gpx-csv/blob/master/tcx2csv.py

# with ...
#   more sensible namespace handling
#   power, cadence, speed, distance, ... values added to output

import argparse
import xml.etree.ElementTree as et

# takes in a TCX file and outputs a CSV file
def main(input, output):
    # iterparse to get the namespaces
    my_namespaces = dict([node for _, node in et.iterparse(input, events=['start-ns'])])

    # we need a couple of namespaces ...
    ns = '{' + my_namespaces.get('') + '}'
    ns3 = '{' + my_namespaces.get('ns3') + '}'

    # parse the XML itself
    tree = et.parse(input)
    root = tree.getroot()

    # ready to process!
    if root.tag != ns + 'TrainingCenterDatabase':
        print('Unknown root found: ' + root.tag)
        return
    activities = root.find(ns + 'Activities')
    if not activities:
        print('Unable to find Activities under root')
        return
    activity = activities.find(ns + 'Activity')
    if not activity:
        print('Unable to find Activity under Activities')
        return
    columnsEstablished = False
    for lap in activity.iter(ns + 'Lap'):
        if columnsEstablished:
            fout.write('New Lap\n')
        for track in lap.iter(ns + 'Track'):
            # pdb.set_trace()
            if columnsEstablished:
                fout.write('New Track\n')
            for trackpoint in track.iter(ns + 'Trackpoint'):
                try:
                    time = trackpoint.find(ns + 'Time').text.strip()
                except:
                    time = ''
                try:
                    latitude = trackpoint.find(ns + 'Position').find(ns + 'LatitudeDegrees').text.strip()
                except:
                    latitude = ''
                try:
                    longitude = trackpoint.find(ns + 'Position').find(ns + 'LongitudeDegrees').text.strip()
                except:
                    longitude = ''
                try:
                    altitude = trackpoint.find(ns + 'AltitudeMeters').text.strip()
                except:
                    altitude = ''
                try:
                    bpm = trackpoint.find(ns + 'HeartRateBpm').find(ns + 'Value').text.strip()
                except:
                    bpm = ''
                try:
                    cadence = trackpoint.find(ns + 'Cadence').text.strip()
                except:
                    cadence = ''
                try:
                    dist = trackpoint.find(ns + 'DistanceMeters').text.strip()
                except:
                    dist = ''
                try:
                    speed = trackpoint.find(ns + 'Extensions').find(ns3 + 'TPX').find(ns3 + 'Speed').text.strip()
                except:
                    speed = 'SPEED'
                try:
                    watts = trackpoint.find(ns + 'Extensions').find(ns3 + 'TPX').find(ns3 + 'Watts').text.strip()
                except:
                    watts = 'SPEED'
                if not columnsEstablished:
                    fout = open(output, 'w')
                    fout.write(','.join(
                        ('Time', 'DistanceMeters', 'Cadence', 'SpeedKPH', 'Watts', 'LatitudeDegrees', 'LongitudeDegrees', 'AltitudeMeters', 'HeartRate')) + '\n')
                    columnsEstablished = True
                fout.write(','.join((time, dist, cadence, speed, watts, latitude, longitude, altitude, bpm)) + '\n')

    fout.close()


if __name__ == '__main__':
    # arguments
    parser = argparse.ArgumentParser(description=
                                     'something.')
    parser.add_argument('input', help='input TCX file')
    parser.add_argument('output', help='output CSV file')
    # parser.add_argument('--verbose', help='increase output verbosity', action='store_true')
    args = parser.parse_args()

    main(args.input, args.output)
