PROGS=feedinfo dayevents

all: $(PROGS)

RUSTC=rustc
RUSTARGS=-O -L rust-csv/ -L .

libcsv.stamp: rust-csv/csv.rc rust-csv/csv.rs
	$(RUSTC) $(RUSTARGS) $< && touch $@

libgtfs.stamp: gtfs.rc gtfs.rs libcsv.stamp
	$(RUSTC) $(RUSTARGS) $< && touch $@

feedinfo: feedinfo.rs libcsv.stamp libgtfs.stamp
	$(RUSTC) $(RUSTARGS) $<

dayevents: dayevents.rs libcsv.stamp libgtfs.stamp
	$(RUSTC) $(RUSTARGS) $<

clean:
	rm -f $(PROGS) *.stamp
	rm -rf *.dSYM rust-csv/*.dSYM
	rm -rf rust-csv/libcsv*.dylib libgtfs*.dylib

