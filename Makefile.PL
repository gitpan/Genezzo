use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'Genezzo',
    'VERSION_FROM'	=> 'lib/Genezzo/GenDBI.pm', # finds $VERSION
    'PREREQ_PM'		=> {

#        'SQL::Statement' => '0.1011', # now use feeble
        'Text::ParseWords' => '0.0',
        'Term::ReadLine'   => '0.0',
        'File::Path'       => '0.0',
        'File::Spec'       => '0.0'
#        'POSIX'            => '0.0'
    }, # e.g., Module::Name => 1.1

   'EXE_FILES' => [ 'lib/Genezzo/gendba.pl' ], # install in /usr/bin
);
