use strict;
use warnings;
use Time::Local 'timelocal';

my $filename = 'twitter_anomalies_timeseries.csv';
open(my $fh, '<:encoding(UTF-8)', $filename) or die "Could not open file '$filename' $!";
my @data;
foreach (1..2){
    while (my $row = <$fh>) {
        if ($row !~ /timestamp/) {
            my @line = split(",",$row);
            my $count = int($line[2]);
            push(@data, $count);
        }
    }
}
close($fh);

my $fileout = 'st1-test.csv';
open(my $out, '>:encoding(UTF-8)', $fileout) or die "Could not open file '$fileout' $!";

my $sampleId = 0;
my $now = time;
my $startTS = $now - 14*24*60*60;
my @buffer;
my $buffercount = 0;
my $buffersum = 0;
my $buffermin = 0;
foreach my $stationId (1) {
    my $di = int(rand(scalar(@data)));
    for(my $t=$startTS;$t<=$now;$t=$t+60) {
        $buffermin++;
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($t);
        my $multiplier=1;
        if ($wday==0 || $wday==6) {
            $multiplier = 0.6;
        }
        my $count = $data[$di] + $stationId * 100;
        $count = $count * $multiplier;

        $buffercount++;
        $buffersum+=$count;
        push (@buffer,$t);
        if ($buffermin>=15) {
            my $avg =int($buffersum/$buffercount);
            foreach my $uxtime (@buffer) {
                print $out "$sampleId,$uxtime,$avg\n";
                $sampleId++;
            }
            @buffer = ();
            $buffercount = 0;
            $buffersum = 0;
            $buffermin = 0;
        }
        $di++;
        $di = 0 if ($di>=scalar(@data));
    }
}
close($out);