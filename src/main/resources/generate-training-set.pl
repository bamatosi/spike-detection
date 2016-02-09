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

my $fileout = 'multiple_wd_ts.csv';
open(my $out, '>:encoding(UTF-8)', $fileout) or die "Could not open file '$fileout' $!";

my $sampleId = 0;
my $now = time;
my $startTS = $now - 14*24*60*60;
foreach my $stationId (1..3) {
    my $di = int(rand(scalar(@data)));
    for(my $t=$startTS;$t<=$now;$t=$t+60) {
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($t);
        my $multiplier=1;
        if ($wday==0 || $wday==6) {
            $multiplier = 0.6;
        }
        my $count = $data[$di] + $stationId * 100;
        $count = $count * $multiplier;
        print $out "$sampleId,$stationId,$t,$count\n";
        $sampleId++;
        $di++;
        $di = 0 if ($di>=scalar(@data));
    }
}
close($out);
