struct UserProfile {
    1:i64 userId
    2:i32 movieWatchedNumRecently
}

service EngineService {
    bool UpdateEngineGroup(1:string groupConf);
    i64 Predict(1:UserProfile userProfile, 2:i64 movieId);
    list<i64> Recommend(1:UserProfile userProfile, 2:i32 topk);
}