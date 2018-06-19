'''
Simple testing workflow

    +------+
    |Odds  +------|
    +------+    +-v----+
                |Double+-------+
    +------+    +-^----+       |
    |Evens +------|          +-v--+     +----+
    +------+                 |Join+----->Save|
                             +-^--+     +----+
    +------+                   |
    |Names +-------------------+
    +------+

'''


from netl import EtlWorkflow, EtlComponent, EtlInput, EtlOutput, EtlRecord, NoMoreData, LOG_DEBUG


class Odds(EtlComponent):

    numbers = EtlOutput()

    def run(self):
        for i in range(1, 10, 2):
            rec = EtlRecord(
                record_type = 'number',
                number = i,
                tag = 'odd',
            )
            self.numbers.output(rec)


class Evens(EtlComponent):

    numbers = EtlOutput()

    def run(self):
        for i in range(2, 11, 2):
            rec = EtlRecord(
                record_type = 'number',
                number = i,
                tag = 'even',
            )
            self.numbers.output(rec)


class Names(EtlComponent):

    names = EtlOutput()

    def run(self):
        self.names.output(EtlRecord('name', name="Zola Rugh"))
        self.names.output(EtlRecord('name', name="Lisha Hanover"))
        self.names.output(EtlRecord('name', name="Kristle Wee"))
        self.names.output(EtlRecord('name', name="Jefferey Brien"))
        self.names.output(EtlRecord('name', name="Gertha Vitolo"))
        self.names.output(EtlRecord('name', name="Many Elkington"))
        self.names.output(EtlRecord('name', name="Rayford Cripe"))
        self.names.output(EtlRecord('name', name="Tonda Lubin"))
        self.names.output(EtlRecord('name', name="Mee Geers"))
        self.names.output(EtlRecord('name', name="Cassey Ruther"))


class Double(EtlComponent):

    numbers = EtlInput()
    output = EtlOutput()

    def run(self):
        for num in self.numbers.all():
            doub = num.copy()
            doub['number'] = doub['number'] * 2
            self.output.output(doub)


class Join(EtlComponent):

    numbers = EtlInput()
    names = EtlInput()
    joined = EtlOutput()

    def run(self):
        try:
            while True:
                num = self.numbers.get()
                name = self.names.get()

                joined = EtlRecord(
                    record_type = 'joined',
                    num = num['number'],
                    tag = num['tag'],
                    name = name['name'],
                )

                self.joined.output(joined)

        except NoMoreData:
            pass


class Save(EtlComponent):

    records = EtlInput()

    def run(self):
        with open('test.saved.txt', 'wt') as fh:
            for rec in self.records.all():
                fh.write("\t".join((rec['name'], str(rec['num']))))


if __name__ == '__main__':

    wf = EtlWorkflow()

    wf.odds = Odds()
    wf.evens = Evens()

    wf.double = Double()
    wf.odds.numbers.connect(wf.double.numbers)
    wf.evens.numbers.connect(wf.double.numbers)

    wf.names = Names()

    wf.join = Join()
    wf.double.output.connect(wf.join.numbers)
    wf.names.names.connect(wf.join.names)

    wf.save = Save()
    wf.save.records.connect(wf.join.joined)

    wf.trace_to('test.trace.zip')
    wf.log_to_console(LOG_DEBUG)

    wf.start()
    wf.wait()

    print("Finished")

