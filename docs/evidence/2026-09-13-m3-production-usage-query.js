// Read-only evidence query. QS_AUDIT_CONNECTION is supplied in memory on the server.
// No credentials or subject identifiers are included in this source or its output.

try {
 const v=QS_AUDIT_CONNECTION;
 const uri='mongodb://'+encodeURIComponent(v.QS_USAGE_USERNAME)+':'+encodeURIComponent(v.QS_USAGE_PASSWORD)+'@'+v.QS_USAGE_HOST+'/'+encodeURIComponent(v.QS_USAGE_DATABASE)+'?authSource=admin&directConnection=true&serverSelectionTimeoutMS=5000&connectTimeoutMS=5000&appName=qs-ai-readonly-migration-audit';
 const database=new Mongo(uri).getDB(v.QS_USAGE_DATABASE);
 const aggregate=(name,pipeline)=>database.getCollection(name).aggregate(pipeline,{maxTimeMS:10000,allowDiskUse:false}).toArray();
 const group=(name,fields,extra={})=>aggregate(name,[{$group:{_id:fields,count:{$sum:1},first_created:{$min:'$created_at'},last_created:{$max:'$created_at'},...extra}}]);
 const size=(field)=>({$size:{$ifNull:['$'+field,[]]}});
 const out={observed_at:new Date().toISOString(),scope:'aggregate_only_no_subjects_or_bodies',profiles:aggregate('ai_explanation_profiles',[
  {$project:{_id:0,status:1,fingerprint:1,profile_id:'$definition.profile_id',version:'$definition.version',selector:'$definition.selector',generation_policy:'$definition.generation_policy',published_at:1,disabled_at:1}},{$limit:101}]),
 evaluations:group('ai_explanation_prompt_evaluations',{evidence_version:{$ifNull:['$evidence_version','v1']},status:'$status'},
 {review_count:{$sum:size('reviews')},human_review_count:{$sum:size('human_reviews')},reopening_rounds:{$sum:size('review_reopenings')},unknown_resolution_count:{$sum:size('result_unknown_resolutions')},unknown_count:{$sum:{$ifNull:['$unresolved_result_unknown_count',0]}},legacy_recovery_count:{$sum:size('recoveries')},with_checkpoint:{$sum:{$cond:[{$ne:[{$ifNull:['$execution',null]},null]},1,0]}}}),
 evaluation_rechecks:group('ai_explanation_prompt_evaluation_rechecks',{status:'$status'}),
 generations:group('ai_explanation_generations',{status:'$status',profile:'$profile',audience:'$audience',prompt:'$prompt',execution_spec:'$execution_spec'}),
 generation_runs:group('ai_explanation_runs',{status:'$status',invocation_phase:'$invocation_phase'}, {attempt_gt_one:{$sum:{$cond:[{$gt:['$attempt',1]},1,0]}},retry_authorized:{$sum:{$cond:[{$ne:[{$ifNull:['$retry_authorization',null]},null]},1,0]}}}),
 evaluation_budgets:aggregate('ai_explanation_prompt_evaluation_daily_budgets',[{$group:{_id:null,documents:{$sum:1},reserved:{$sum:'$reserved_provider_invocations'},reservations:{$sum:size('reservations')},earliest:{$min:'$budget_day'},latest:{$max:'$budget_day'}}}]),
 participant_budgets:aggregate('ai_explanation_participant_daily_budgets',[{$group:{_id:null,documents:{$sum:1},reserved:{$sum:'$reserved_provider_invocations'},reservations:{$sum:size('reservations')},earliest:{$min:'$budget_day'},latest:{$max:'$budget_day'}}}]),
 active_capacity:aggregate('ai_explanation_participant_active_capacity',[{$group:{_id:null,documents:{$sum:1},active:{$sum:'$active_executions'},reservations:{$sum:size('reservations')}}}])};
 out.published_evaluation_links=aggregate('ai_explanation_profiles',[
  {$match:{status:'published'}},
  {$lookup:{from:'ai_explanation_prompt_evaluations',localField:'published_evidence_run_id',foreignField:'domain_id',as:'linked'}},
  {$project:{
   _id:0,profile_id:'$definition.profile_id',version:'$definition.version',matched_evaluations:{$size:'$linked'},
   evaluation_statuses:'$linked.status',evidence_versions:'$linked.evidence_version',
   reopening_counts:{$map:{input:'$linked',as:'r',in:{$size:{$ifNull:['$$r.review_reopenings',[]]}}}},
   human_review_counts:{$map:{input:'$linked',as:'r',in:{$size:{$ifNull:['$$r.human_reviews',[]]}}}}
  }},{$limit:101}
 ]);
 print('QS_USAGE_RESULT:'+JSON.stringify(out));
} catch(e) { print('QS_USAGE_RESULT:'+JSON.stringify({error:'readonly_query_failed',code:e.code||null,name:e.name,network_class:['ENOTFOUND','ECONNREFUSED','ETIMEDOUT','EHOSTUNREACH','TLS','SSL','getaddrinfo'].filter(x=>String(e.message).includes(x))})); quit(1); }
