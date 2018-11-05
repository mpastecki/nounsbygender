#!/usr/bin/env python

"""
Copyright Marcin Pastecki
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import apache_beam as beam
import sys
import re

class AddTypeOfWord(beam.DoFn):
    def process(self, element):
        if len(element.split('\t')) < 4:
            yield element + '\tunknown'
        else:
            yield element

class TransformPolimorf(beam.DoFn):
    def process(self, element):
        word, baseform, meta, noun_type = element.split('\t')

        part = ''
        noun_number = ''
        noun_gender = ''

        try:
            pattern = re.compile('^(?:[^:]*\:){0}([^:]*)')
            part = pattern.search(meta).group(1)
        except AttributeError:
            part = 'unknown'

        if part == 'subst' or part == 'depr':
            try:
                pattern = re.compile('^(?:[^:]*\:){1}([^:]*)')
                noun_number = pattern.search(meta).group(1)
            except AttributeError:
                noun_number = 'unknown'

            try:
                pattern = re.compile('^(?:[^:]*\:){3}([^:]*)')
                noun_gender = pattern.search(meta).group(1)
            except AttributeError:
                noun_gender = 'unknown'

        yield ('{};{};{};{};{};{}'.format(word, baseform, part, noun_number, noun_gender, noun_type))

class FilterNouns(beam.DoFn):
    def process(self, element):
        form = element.split(';')[2]
        if form == 'subst' or form == 'depr':
            yield element

if __name__ == '__main__':

   reload(sys)
   sys.setdefaultencoding('utf8')

   p = beam.Pipeline(argv=sys.argv)
   input = './data/slownik.tab'
   output_prefix = './data/polimorf.output'

   # Let's get our dictionary shaped according to our needs
   (p
       | 'ReadFromText' >> beam.io.ReadFromText(input)
#       | 'DecodeUnicode' >> beam.FlatMap(lambda encoded: encoded.decode('utf-8'))
       | 'AddMissingData' >> beam.ParDo(AddTypeOfWord())
       | 'ExtractData' >> beam.ParDo(TransformPolimorf())
       | 'RemveNotNouns' >> beam.ParDo(FilterNouns())
       | 'WriteToText' >> beam.io.WriteToText(output_prefix)
   )

   p.run().wait_until_finish()
