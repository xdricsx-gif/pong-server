'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {ZONE,day,shiftDay,date,stamp}=require('./dashboard-time');
const {day:filterDay,aggregateInstalls}=require('./dashboard-data');
test('Kyiv date keys use local midnight, independently of host timezone',()=>{
 assert.equal(ZONE,'Europe/Kyiv');assert.equal(day('2026-10-24T20:59:59Z'),'2026-10-24');assert.equal(day('2026-10-24T21:00:00Z'),'2026-10-25');
 assert.equal(day('2026-12-01T21:59:59Z'),'2026-12-01');assert.equal(day('2026-12-01T22:00:00Z'),'2026-12-02');
 assert.equal(filterDay({toDate:()=>new Date('2026-10-24T21:15:00Z')}),'2026-10-25');assert.equal(stamp('2026-10-24T21:15:00Z'),'25.10.26, 00:15');
});
test('DST fall day contains 25 hours; both repeated hours stay on October 25',()=>{
 for(const time of ['2026-10-24T21:00:00Z','2026-10-25T00:30:00Z','2026-10-25T01:30:00Z','2026-10-25T21:59:59Z'])assert.equal(day(time),'2026-10-25');
 assert.equal(day('2026-10-25T22:00:00Z'),'2026-10-26');assert.equal(Date.parse('2026-10-25T22:00Z')-Date.parse('2026-10-24T21:00Z'),25*3600000);
 assert.equal(shiftDay(day('2026-10-25T22:30:00Z'),-1),'2026-10-25');assert.equal(shiftDay('2026-10-26',-29),'2026-09-27');
});
test('DST spring day contains 23 hours and ranges subtract calendar days',()=>{
 assert.equal(day('2026-03-28T21:59:59Z'),'2026-03-28');assert.equal(day('2026-03-28T22:00:00Z'),'2026-03-29');assert.equal(day('2026-03-29T20:59:59Z'),'2026-03-29');assert.equal(day('2026-03-29T21:00:00Z'),'2026-03-30');
 assert.equal(shiftDay('2026-03-30',-1),'2026-03-29');assert.equal(shiftDay('2027-01-01',-1),'2026-12-31');
});
test('official Google Play dates remain literal, invalid and missing dates stay missing',()=>{
 assert.equal(day('2026-10-25'),'2026-10-25');assert.equal(date('2026-10-25'),'25.10.2026');assert.equal(day('2026-02-30'),null);assert.equal(day(null),null);assert.equal(day(undefined),null);assert.equal(day('bad'),null);assert.equal(day({toDate(){throw Error('bad');}}),null);assert.equal(stamp(null),'—');
 const a={date:'2026-10-24',country:'UA',installs:1,uninstalls:0,installed:3};assert.equal(aggregateInstalls([a,{...a,date:'2026-10-25',installs:2}],'2026-10-25','2026-10-25').installs,2);
});
