#!/bin/perl

use POSIX qw(strftime);
#
#
#
opendir(DIR, '.');
$x = 0;

while (my $file = readdir(DIR))
{
    $chfile[$x] = $file;
    $x = $x + 1;
}

foreach(@chfile)
{
   #print $file . "\n";
      $file = $_;
      ($aspect,$tscsv) = split('-', $file);
      ($ts,$csv) = split('\.', $tscsv);
      $newts = strftime("%Y%m%d_%H%M",localtime($ts));
      $newfilename = $aspect . "__" . $newts . ".csv"; 
      print "renaming $file to $newfilename\n";
      system("mv $file $newfilename");
}
