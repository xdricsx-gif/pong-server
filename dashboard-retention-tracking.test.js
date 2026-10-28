'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {retentionPresenceFields}=require('./dashboard-retention-tracking');
test('login uses Kyiv calendar and retains old exact activity evidence',()=>{
 const saved={activeDays:{'2025-01-01':true,'2026-10-24':true},other:1};
 assert.deepEqual(retentionPresenceFields(saved,'2026-10-25T22:00:00Z'),{activeDays:{'2025-01-01':true,'2026-10-24':true,'2026-10-26':true}});
 assert.deepEqual(saved,{activeDays:{'2025-01-01':true,'2026-10-24':true},other:1});
});
test('only real nonfuture calendar dates and boolean day evidence are retained',()=>{
 const saved={activeDays:{'2026-02-30':true,'2026-02-28':true,'2099-01-01':true,'2026-10-27':'true',unknown:true}};
 assert.deepEqual(retentionPresenceFields(saved,'2026-10-28T12:00:00Z',['2026-10-24','invalid','2099-01-01']),{activeDays:{'2026-02-28':true,'2026-10-24':true}});
 assert.throws(()=>retentionPresenceFields({},'invalid'),/invalid_activity_time/);
});
test('delayed persistence writes observed days rather than the flush day',()=>{
 assert.deepEqual(retentionPresenceFields({},'2026-10-28T12:00:00Z',['2026-10-24']),{activeDays:{'2026-10-24':true}});
 assert.deepEqual(retentionPresenceFields({},'2026-10-28T12:00:00Z',[]),{activeDays:{}});
});
