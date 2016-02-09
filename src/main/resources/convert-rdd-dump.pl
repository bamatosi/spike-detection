use strict;
use warnings;
use Time::Local 'timelocal';
use Time::Piece;

my $filename = 'st1-analysis/part-00000';
my $fileout = 'st1-test-clean.csv';
open(my $out, '>:encoding(UTF-8)', $fileout) or die "Could not open file '$fileout' $!";
open(my $fh, '<:encoding(UTF-8)', $filename) or die "Could not open file '$filename' $!";

print $out "id,datetime,rushind\n";
my $sampleId = 1;
while (my $row = <$fh>) {
    $row =~ s/^\(//;
    $row =~ s/\)$//;
    my @line = split(",",$row);
    my $time = localtime($line[0])->strftime('%F %T');
    my $rush = $line[1]*100;
    print $out '"'.$sampleId.'"'.",$time,$rush\n";
    $sampleId++;
}

close($fh);
close($out);