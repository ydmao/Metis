Q	:= @
CC      := g++ -std=gnu++0x
DEPSDIR := .deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
TOP	:= $(shell echo $${PWD-'pwd'})
ARCH 	:= $(shell uname -m | sed -e s/x86_64/amd64/ -e s/i.86/i386/)
MAXCPUS := $(shell grep -c processor /proc/cpuinfo)
O       := obj

ifdef DEBUG
  OPTFLAGS := -g -O0 -fno-omit-frame-pointer
else
  OPTFLAGS := -g -O3
endif

CFLAGS	:= -D_GNU_SOURCE -Wall $(OPTFLAGS) \
	   -I$(TOP) -I$(TOP)/lib -DJTLS=__thread -DJSHARED_ATTR=  \
	   -DJOS_CLINE=64 -DCACHE_LINE_SIZE=64 \
	   -DJOS_NCPU=$(MAXCPUS) -D__STDC_FORMAT_MACROS

COMMON_LIB := -lc -lm -lpthread -lrt -ldl
LIB	   := -L$(O) -lmetis $(COMMON_LIB)
SF_LIB	   := -L$(O) -lmetis -ldl -lflow -lnuma $(COMMON_LIB)

LDEPS := $(O)/libmetis.a

# Which MapReduce applications to build?  .sf binaries will be
# linked against libflow
PROGS := obj/kmeans 			    \
	 obj/matrix_mult 		    \
	 obj/pca 			    \
	 obj/wc 			    \
	 obj/wr				    \
	 obj/linear_regression		    \
	 obj/hist			    \
	 obj/string_match		    \
	 obj/wrmem			    \
         obj/matrix_mult2                   \
	 obj/sf_sample                    \
         obj/btree_unit

PROGS += $(addsuffix .sf,$(PROGS))

all: $(PROGS)

include lib/Makefrag

$(O)/%: $(O)/%.o $(LDEPS) $(DEPSDIR)/stamp
	@echo "MAKE	$@"
	$(Q)$(CC) $(DEPCFLAGS) $(CFLAGS) -o $@ $< $(LIB)

$(O)/%.o: app/%.cc $(DEPSDIR)/stamp
	$(Q)mkdir -p $(@D)
	@echo "CC	$<"
	$(Q)$(CC) $(DEPCFLAGS) $(CFLAGS) -o $@ -c $<

$(O)/%.o: micro/%.cc $(DEPSDIR)/stamp
	$(Q)mkdir -p $(@D)
	$(Q)$(CC) $(DEPCFLAGS) $(CFLAGS) -o $@ -c $<

$(O)/%.sf: $(O)/%.o $(LDEPS) $(DEPSDIR)/stamp
	@echo "MAKE	$@"
	$(Q)$(CC) $(DEPCFLAGS) $(CFLAGS) -o $@ $< $(SF_LIB)

$(DEPSDIR)/stamp:
	@echo "MAKE	$@"
	@mkdir -p $(@D)
	@touch $@ 

DTOP = ./data
sanity_data:
	dd if=/dev/urandom of=$(DTOP)/lr_10MB.txt count=1024 bs=10240

data_gen: $(DTOP)/lr_10MB.txt
	cd data_tool && g++ gen.cc -o gen
	bash data_tool/data-gen.sh

data_clean:
	rm data_tool/gen -rf
	bash data_tool/clean.sh

clean:
	@rm -rf $(PROGS) *.o *.a *~ *.tmp *.bak *.log *.orig $(O) $(DEPSDIR)

.PRECIOUS: $(O)/%.o

DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif

.PHONY: default clean
