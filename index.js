import fs from 'node:fs';
import path from 'node:path';

import { parse } from 'csv-parse';
import Knex from 'knex';

const files = ['file1.csv', 'file2.csv']; // file order defines importing order

async function run() {
   const db = Knex({
      client: 'pg',
      connection: {
         host: '127.0.0.1',
         port: 54322,
         user: 'postgres',
         password: 'postgrespw',
         database: 'yellowpages'
      }
   });


   const columns = files.map(file => file.split('.')[0])
   const defs = columns.map(column => `${column} int`).join(', ');
   await db.raw('drop table if exists yellowpages;')
   await db.raw(`create table yellowpages ( id varchar not null primary key, ${defs})`)

   for (const file of files) {
      let rownumber = 0;
      const filepath = path.resolve('.', 'source', file);
      const parser = parse({
         delimiter: ',',
         columns: true
      });
      const filestream = fs.createReadStream(filepath);
      const parsed = filestream.pipe(parser);
      for await (const record of parsed) {
         const column = file.split('.')[0];
         await db.raw(`insert into yellowpages (id, ??) values(?, ?) ON CONFLICT (id) DO UPDATE SET ?? = ?`, [column, record.phone, rownumber, column, rownumber])
         rownumber++;

         process.stdout.write(`Processing ${file}:${rownumber}\r`)
      }
   }

   await db.destroy();
}

run()
