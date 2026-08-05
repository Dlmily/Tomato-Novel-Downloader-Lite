.class public abstract Lms/bd/c/u2;
.super Ljava/lang/Object;
.source "SourceFile"


# direct methods
.method static constructor <clinit>()V
    .locals 1

    const v0, 0x9cc3e

    invoke-static {v0}, Lcom/bytedance/covode/number/Covode;->recordClassIndex(I)V

    return-void
.end method

.method public static a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;
    .locals 31

    .prologue
    .line 84475904
    move/from16 v0, p0

    .line 84475905
    .line 84475906
    const/high16 v1, 0x10000000

    .line 84475907
    .line 84475908
    if-le v0, v1, :cond_4

    .line 84475909
    .line 84475910
    const v1, 0x10000013

    .line 84475911
    .line 84475912
    .line 84475913
    if-ge v0, v1, :cond_4

    .line 84475914
    .line 84475915
    packed-switch v0, :pswitch_data_0

    .line 84475916
    .line 84475917
    .line 84475918
    :goto_0
    :pswitch_0
    const/4 v0, 0x0

    .line 84475919
    goto/16 :goto_4

    .line 84475920
    .line 84475921
    :pswitch_1
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84475922
    .line 84475923
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84475924
    .line 84475925
    invoke-static {v0}, Lms/bd/c/k1;->c(Landroid/content/Context;)Lms/bd/c/k1;

    .line 84475926
    .line 84475927
    .line 84475928
    move-result-object v0

    .line 84475929
    iget-object v0, v0, Lms/bd/c/k1;->c:Ljava/lang/String;

    .line 84475930
    .line 84475931
    goto/16 :goto_4

    .line 84475932
    .line 84475933
    :pswitch_2
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84475934
    .line 84475935
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84475936
    .line 84475937
    invoke-static {v0}, Lms/bd/c/k1;->c(Landroid/content/Context;)Lms/bd/c/k1;

    .line 84475938
    .line 84475939
    .line 84475940
    move-result-object v0

    .line 84475941
    invoke-virtual {v0}, Ljava/lang/Object;->getClass()Ljava/lang/Class;

    .line 84475942
    .line 84475943
    .line 84475944
    goto :goto_0

    .line 84475945
    :pswitch_3
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84475946
    .line 84475947
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84475948
    .line 84475949
    invoke-static {v0}, Lms/bd/c/k1;->c(Landroid/content/Context;)Lms/bd/c/k1;

    .line 84475950
    .line 84475951
    .line 84475952
    move-result-object v0

    .line 84475953
    iget-object v0, v0, Lms/bd/c/k1;->b:Ljava/lang/String;

    .line 84475954
    .line 84475955
    goto/16 :goto_4

    .line 84475956
    .line 84475957
    :pswitch_4
    invoke-static {}, Lcom/bytedance/mobsec/metasec/ml/MSC;->GetDelayTime()J

    .line 84475958
    .line 84475959
    .line 84475960
    move-result-wide v0

    .line 84475961
    invoke-static {v0, v1}, Ljava/lang/Long;->valueOf(J)Ljava/lang/Long;

    .line 84475962
    .line 84475963
    .line 84475964
    move-result-object v0

    .line 84475965
    goto/16 :goto_4

    .line 84475966
    .line 84475967
    :pswitch_5
    invoke-static {}, Lcom/bytedance/mobsec/metasec/ml/MSC;->GetABSwitch()J

    .line 84475968
    .line 84475969
    .line 84475970
    move-result-wide v0

    .line 84475971
    invoke-static {v0, v1}, Ljava/lang/Long;->valueOf(J)Ljava/lang/Long;

    .line 84475972
    .line 84475973
    .line 84475974
    move-result-object v0

    .line 84475975
    goto/16 :goto_4

    .line 84475976
    .line 84475977
    :pswitch_6
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84475978
    .line 84475979
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84475980
    .line 84475981
    if-nez v0, :cond_0

    .line 84475982
    .line 84475983
    goto :goto_0

    .line 84475984
    :cond_0
    new-instance v1, Ljava/lang/StringBuilder;

    .line 84475985
    .line 84475986
    invoke-static {v0}, Lms/bd/c/s3;->b(Landroid/content/Context;)Ljava/lang/String;

    .line 84475987
    .line 84475988
    .line 84475989
    move-result-object v0

    .line 84475990
    invoke-direct {v1, v0}, Ljava/lang/StringBuilder;-><init>(Ljava/lang/String;)V

    .line 84475991
    .line 84475992
    .line 84475993
    invoke-virtual {v1}, Ljava/lang/StringBuilder;->toString()Ljava/lang/String;

    .line 84475994
    .line 84475995
    .line 84475996
    move-result-object v0

    .line 84475997
    goto/16 :goto_4

    .line 84475998
    .line 84475999
    :pswitch_7
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84476000
    .line 84476001
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84476002
    .line 84476003
    invoke-static {v0}, Lms/bd/c/k1;->c(Landroid/content/Context;)Lms/bd/c/k1;

    .line 84476004
    .line 84476005
    .line 84476006
    move-result-object v0

    .line 84476007
    iget-object v0, v0, Lms/bd/c/k1;->a:Ljava/lang/String;

    .line 84476008
    .line 84476009
    goto/16 :goto_4

    .line 84476010
    .line 84476011
    :pswitch_8
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84476012
    .line 84476013
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84476014
    .line 84476015
    invoke-static {v0}, Lms/bd/c/k1;->c(Landroid/content/Context;)Lms/bd/c/k1;

    .line 84476016
    .line 84476017
    .line 84476018
    goto :goto_0

    .line 84476019
    :pswitch_9
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84476020
    .line 84476021
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84476022
    .line 84476023
    invoke-static {v0}, Lms/bd/c/s3;->d(Landroid/content/Context;)Ljava/lang/String;

    .line 84476024
    .line 84476025
    .line 84476026
    move-result-object v0

    .line 84476027
    goto/16 :goto_4

    .line 84476028
    .line 84476029
    :pswitch_a
    :try_start_0
    invoke-static {}, Lms/bd/c/k3;->a()Z
    :try_end_0
    .catchall {:try_start_0 .. :try_end_0} :catchall_0

    .line 84476030
    .line 84476031
    .line 84476032
    sget v0, Lms/bd/c/n4;->a:I

    .line 84476033
    .line 84476034
    goto :goto_1

    .line 84476035
    :catchall_0
    sget v0, Lms/bd/c/n4;->a:I

    .line 84476036
    .line 84476037
    :goto_1
    const-string v0, "null[<!>]null[<!>]0[<!>]"

    .line 84476038
    .line 84476039
    goto/16 :goto_4

    .line 84476040
    .line 84476041
    :pswitch_b
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84476042
    .line 84476043
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84476044
    .line 84476045
    new-instance v1, Lorg/json/JSONArray;

    .line 84476046
    .line 84476047
    invoke-direct {v1}, Lorg/json/JSONArray;-><init>()V

    .line 84476048
    .line 84476049
    .line 84476050
    if-nez v0, :cond_1

    .line 84476051
    .line 84476052
    invoke-virtual {v1}, Lorg/json/JSONArray;->toString()Ljava/lang/String;

    .line 84476053
    .line 84476054
    .line 84476055
    move-result-object v0

    .line 84476056
    goto/16 :goto_4

    .line 84476057
    .line 84476058
    :cond_1
    :try_start_1
    invoke-static {}, Lms/bd/c/k3;->a()Z

    .line 84476059
    .line 84476060
    .line 84476061
    move-result v0

    .line 84476062
    if-eqz v0, :cond_2

    .line 84476063
    .line 84476064
    invoke-virtual {v1}, Lorg/json/JSONArray;->toString()Ljava/lang/String;

    .line 84476065
    .line 84476066
    .line 84476067
    move-result-object v0
    :try_end_1
    .catchall {:try_start_1 .. :try_end_1} :catchall_1

    .line 84476068
    goto/16 :goto_4

    .line 84476069
    .line 84476070
    :catchall_1
    :cond_2
    const-string v0, "[]"

    .line 84476071
    .line 84476072
    goto/16 :goto_4

    .line 84476073
    .line 84476074
    :pswitch_c
    sget-object v0, Lms/bd/c/w;->a:Ljava/lang/String;

    .line 84476075
    .line 84476076
    new-instance v0, Lorg/json/JSONObject;

    .line 84476077
    .line 84476078
    invoke-direct {v0}, Lorg/json/JSONObject;-><init>()V

    .line 84476079
    .line 84476080
    .line 84476081
    const/4 v1, 0x2

    .line 84476082
    :try_start_2
    invoke-static {}, Lms/bd/c/w;->c()Ljava/util/HashMap;

    .line 84476083
    .line 84476084
    .line 84476085
    move-result-object v2

    .line 84476086
    sget-object v3, Lms/bd/c/w;->i:Ljava/lang/String;
    :try_end_2
    .catchall {:try_start_2 .. :try_end_2} :catchall_3

    .line 84476087
    .line 84476088
    const/16 v5, 0xa

    .line 84476089
    .line 84476090
    const/16 v6, 0x8

    .line 84476091
    .line 84476092
    const/16 v7, 0x35

    .line 84476093
    .line 84476094
    const/4 v8, 0x4

    .line 84476095
    const/16 v9, 0x16

    .line 84476096
    .line 84476097
    const/4 v10, 0x3

    .line 84476098
    const/16 v11, 0x28

    .line 84476099
    .line 84476100
    const/4 v12, 0x0

    .line 84476101
    const/16 v14, 0x10

    .line 84476102
    .line 84476103
    const/16 v15, 0xc

    .line 84476104
    .line 84476105
    const/16 v16, 0x7

    .line 84476106
    .line 84476107
    const/16 v17, 0x5

    .line 84476108
    .line 84476109
    const/16 v18, 0xe

    .line 84476110
    .line 84476111
    const/16 v19, 0x6

    .line 84476112
    .line 84476113
    :try_start_3
    new-instance v4, Lcom/bytedance/security/android/aopcheck/PolarisFileWrapper;

    .line 84476114
    .line 84476115
    const v20, 0x1000001

    .line 84476116
    .line 84476117
    .line 84476118
    const/16 v21, 0x0

    .line 84476119
    .line 84476120
    const-wide/16 v22, 0x0

    .line 84476121
    .line 84476122
    const-string v24, "c9d5f1"

    .line 84476123
    .line 84476124
    const/16 v13, 0x18

    .line 84476125
    .line 84476126
    new-array v13, v13, [B

    .line 84476127
    .line 84476128
    const/16 v25, 0x3d

    .line 84476129
    .line 84476130
    aput-byte v25, v13, v12

    .line 84476131
    .line 84476132
    const/16 v25, 0x1

    .line 84476133
    .line 84476134
    aput-byte v11, v13, v25

    .line 84476135
    .line 84476136
    aput-byte v18, v13, v1

    .line 84476137
    .line 84476138
    const/16 v25, 0x52

    .line 84476139
    .line 84476140
    aput-byte v25, v13, v10

    .line 84476141
    .line 84476142
    aput-byte v9, v13, v8

    .line 84476143
    .line 84476144
    const/16 v25, 0x22

    .line 84476145
    .line 84476146
    aput-byte v25, v13, v17

    .line 84476147
    .line 84476148
    const/16 v25, 0x65

    .line 84476149
    .line 84476150
    aput-byte v25, v13, v19

    .line 84476151
    .line 84476152
    aput-byte v18, v13, v16

    .line 84476153
    .line 84476154
    const/16 v25, 0x3c

    .line 84476155
    .line 84476156
    aput-byte v25, v13, v6

    .line 84476157
    .line 84476158
    const/16 v25, 0x9

    .line 84476159
    .line 84476160
    const/16 v26, 0x66

    .line 84476161
    .line 84476162
    aput-byte v26, v13, v25

    .line 84476163
    .line 84476164
    const/16 v25, 0x77

    .line 84476165
    .line 84476166
    aput-byte v25, v13, v5

    .line 84476167
    .line 84476168
    const/16 v25, 0xb

    .line 84476169
    .line 84476170
    aput-byte v11, v13, v25

    .line 84476171
    .line 84476172
    const/16 v25, 0x58

    .line 84476173
    .line 84476174
    aput-byte v25, v13, v15

    .line 84476175
    .line 84476176
    const/16 v25, 0xd

    .line 84476177
    .line 84476178
    const/16 v26, 0x52

    .line 84476179
    .line 84476180
    aput-byte v26, v13, v25

    .line 84476181
    .line 84476182
    const/16 v25, 0x40

    .line 84476183
    .line 84476184
    aput-byte v25, v13, v18

    .line 84476185
    .line 84476186
    const/16 v25, 0xf

    .line 84476187
    .line 84476188
    aput-byte v7, v13, v25

    .line 84476189
    .line 84476190
    const/16 v25, 0x74

    .line 84476191
    .line 84476192
    aput-byte v25, v13, v14

    .line 84476193
    .line 84476194
    const/16 v25, 0x11

    .line 84476195
    .line 84476196
    const/16 v26, 0x1d

    .line 84476197
    .line 84476198
    aput-byte v26, v13, v25

    .line 84476199
    .line 84476200
    const/16 v25, 0x12

    .line 84476201
    .line 84476202
    const/16 v26, 0x38

    .line 84476203
    .line 84476204
    aput-byte v26, v13, v25

    .line 84476205
    .line 84476206
    const/16 v25, 0x2a

    .line 84476207
    .line 84476208
    const/16 v26, 0x13

    .line 84476209
    .line 84476210
    aput-byte v25, v13, v26

    .line 84476211
    .line 84476212
    const/16 v25, 0x14

    .line 84476213
    .line 84476214
    const/16 v26, 0x71

    .line 84476215
    .line 84476216
    aput-byte v26, v13, v25

    .line 84476217
    .line 84476218
    const/16 v25, 0x15

    .line 84476219
    .line 84476220
    const/16 v26, 0x2b

    .line 84476221
    .line 84476222
    aput-byte v26, v13, v25

    .line 84476223
    .line 84476224
    aput-byte v1, v13, v9

    .line 84476225
    .line 84476226
    const/16 v25, 0x17

    .line 84476227
    .line 84476228
    aput-byte v18, v13, v25

    .line 84476229
    .line 84476230
    move-object/from16 v25, v13

    .line 84476231
    .line 84476232
    invoke-static/range {v20 .. v25}, Lms/bd/c/y2;->a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;

    .line 84476233
    .line 84476234
    .line 84476235
    move-result-object v13

    .line 84476236
    check-cast v13, Ljava/lang/String;

    .line 84476237
    .line 84476238
    invoke-direct {v4, v13}, Lcom/bytedance/security/android/aopcheck/PolarisFileWrapper;-><init>(Ljava/lang/String;)V

    .line 84476239
    .line 84476240
    .line 84476241
    sget-object v13, Lms/bd/c/w;->j:Lms/bd/c/v;

    .line 84476242
    .line 84476243
    invoke-virtual {v4, v13}, Ljava/io/File;->listFiles(Ljava/io/FileFilter;)[Ljava/io/File;

    .line 84476244
    .line 84476245
    .line 84476246
    move-result-object v4

    .line 84476247
    array-length v4, v4
    :try_end_3
    .catchall {:try_start_3 .. :try_end_3} :catchall_2

    .line 84476248
    goto :goto_2

    .line 84476249
    :catchall_2
    const/4 v4, -0x1

    .line 84476250
    :goto_2
    :try_start_4
    invoke-virtual {v0, v3, v4}, Lorg/json/JSONObject;->put(Ljava/lang/String;I)Lorg/json/JSONObject;

    .line 84476251
    .line 84476252
    .line 84476253
    sget-object v3, Lms/bd/c/w;->a:Ljava/lang/String;

    .line 84476254
    .line 84476255
    invoke-static {v2, v3}, Lms/bd/c/w;->b(Ljava/util/HashMap;Ljava/lang/String;)Ljava/lang/String;

    .line 84476256
    .line 84476257
    .line 84476258
    move-result-object v3

    .line 84476259
    sget-object v4, Lms/bd/c/w;->d:Ljava/lang/String;

    .line 84476260
    .line 84476261
    invoke-virtual {v0, v4, v3}, Lorg/json/JSONObject;->put(Ljava/lang/String;Ljava/lang/Object;)Lorg/json/JSONObject;

    .line 84476262
    .line 84476263
    .line 84476264
    sget-object v3, Lms/bd/c/w;->b:Ljava/lang/String;

    .line 84476265
    .line 84476266
    invoke-static {v2, v3}, Lms/bd/c/w;->b(Ljava/util/HashMap;Ljava/lang/String;)Ljava/lang/String;

    .line 84476267
    .line 84476268
    .line 84476269
    move-result-object v3

    .line 84476270
    sget-object v4, Lms/bd/c/w;->e:Ljava/lang/String;

    .line 84476271
    .line 84476272
    invoke-virtual {v0, v4, v3}, Lorg/json/JSONObject;->put(Ljava/lang/String;Ljava/lang/Object;)Lorg/json/JSONObject;

    .line 84476273
    .line 84476274
    .line 84476275
    sget-object v3, Lms/bd/c/w;->g:Ljava/lang/String;

    .line 84476276
    .line 84476277
    const v20, 0x1000001

    .line 84476278
    .line 84476279
    .line 84476280
    const/16 v21, 0x0

    .line 84476281
    .line 84476282
    const-wide/16 v22, 0x0

    .line 84476283
    .line 84476284
    const-string v24, "7afa64"

    .line 84476285
    .line 84476286
    new-array v4, v7, [B

    .line 84476287
    .line 84476288
    const/16 v13, 0x69

    .line 84476289
    .line 84476290
    aput-byte v13, v4, v12

    .line 84476291
    .line 84476292
    const/4 v13, 0x1

    .line 84476293
    const/16 v25, 0x70

    .line 84476294
    .line 84476295
    aput-byte v25, v4, v13

    .line 84476296
    .line 84476297
    aput-byte v15, v4, v1

    .line 84476298
    .line 84476299
    aput-byte v19, v4, v10

    .line 84476300
    .line 84476301
    const/16 v13, 0x46

    .line 84476302
    .line 84476303
    aput-byte v13, v4, v8

    .line 84476304
    .line 84476305
    const/16 v13, 0x27

    .line 84476306
    .line 84476307
    aput-byte v13, v4, v17

    .line 84476308
    .line 84476309
    const/16 v25, 0x31

    .line 84476310
    .line 84476311
    aput-byte v25, v4, v19

    .line 84476312
    .line 84476313
    const/16 v25, 0x56

    .line 84476314
    .line 84476315
    aput-byte v25, v4, v16

    .line 84476316
    .line 84476317
    const/16 v25, 0x3e

    .line 84476318
    .line 84476319
    aput-byte v25, v4, v6

    .line 84476320
    .line 84476321
    const/16 v25, 0x9

    .line 84476322
    .line 84476323
    const/16 v26, 0x32

    .line 84476324
    .line 84476325
    aput-byte v26, v4, v25

    .line 84476326
    .line 84476327
    const/16 v27, 0x23

    .line 84476328
    .line 84476329
    aput-byte v27, v4, v5

    .line 84476330
    .line 84476331
    const/16 v25, 0xb

    .line 84476332
    .line 84476333
    const/16 v28, 0x70

    .line 84476334
    .line 84476335
    aput-byte v28, v4, v25

    .line 84476336
    .line 84476337
    const/16 v25, 0x5a

    .line 84476338
    .line 84476339
    aput-byte v25, v4, v15

    .line 84476340
    .line 84476341
    const/16 v25, 0xd

    .line 84476342
    .line 84476343
    aput-byte v19, v4, v25

    .line 84476344
    .line 84476345
    aput-byte v14, v4, v18

    .line 84476346
    .line 84476347
    const/16 v25, 0xf

    .line 84476348
    .line 84476349
    const/16 v28, 0x30

    .line 84476350
    .line 84476351
    aput-byte v28, v4, v25

    .line 84476352
    .line 84476353
    const/16 v25, 0x20

    .line 84476354
    .line 84476355
    aput-byte v25, v4, v14

    .line 84476356
    .line 84476357
    const/16 v25, 0x11

    .line 84476358
    .line 84476359
    const/16 v29, 0x45

    .line 84476360
    .line 84476361
    aput-byte v29, v4, v25

    .line 84476362
    .line 84476363
    const/16 v25, 0x12

    .line 84476364
    .line 84476365
    const/16 v29, 0x3a

    .line 84476366
    .line 84476367
    aput-byte v29, v4, v25

    .line 84476368
    .line 84476369
    const/16 v25, 0x7e

    .line 84476370
    .line 84476371
    const/16 v29, 0x13

    .line 84476372
    .line 84476373
    aput-byte v25, v4, v29

    .line 84476374
    .line 84476375
    const/16 v25, 0x14

    .line 84476376
    .line 84476377
    const/16 v29, 0x25

    .line 84476378
    .line 84476379
    aput-byte v29, v4, v25

    .line 84476380
    .line 84476381
    const/16 v25, 0x15

    .line 84476382
    .line 84476383
    const/16 v30, 0x73

    .line 84476384
    .line 84476385
    aput-byte v30, v4, v25

    .line 84476386
    .line 84476387
    aput-byte v12, v4, v9

    .line 84476388
    .line 84476389
    const/16 v25, 0x5a

    .line 84476390
    .line 84476391
    const/16 v30, 0x17

    .line 84476392
    .line 84476393
    aput-byte v25, v4, v30

    .line 84476394
    .line 84476395
    const/16 v25, 0x18

    .line 84476396
    .line 84476397
    aput-byte v5, v4, v25

    .line 84476398
    .line 84476399
    const/16 v25, 0x19

    .line 84476400
    .line 84476401
    const/16 v30, 0x33

    .line 84476402
    .line 84476403
    aput-byte v30, v4, v25

    .line 84476404
    .line 84476405
    const/16 v25, 0x1a

    .line 84476406
    .line 84476407
    const/16 v30, 0x21

    .line 84476408
    .line 84476409
    aput-byte v30, v4, v25

    .line 84476410
    .line 84476411
    const/16 v25, 0x1b

    .line 84476412
    .line 84476413
    aput-byte v14, v4, v25

    .line 84476414
    .line 84476415
    const/16 v25, 0x1c

    .line 84476416
    .line 84476417
    const/16 v30, 0x78

    .line 84476418
    .line 84476419
    aput-byte v30, v4, v25

    .line 84476420
    .line 84476421
    const/16 v25, 0x1d

    .line 84476422
    .line 84476423
    aput-byte v26, v4, v25

    .line 84476424
    .line 84476425
    const/16 v25, 0x1e

    .line 84476426
    .line 84476427
    const/16 v30, 0x36

    .line 84476428
    .line 84476429
    aput-byte v30, v4, v25

    .line 84476430
    .line 84476431
    const/16 v25, 0x1f

    .line 84476432
    .line 84476433
    const/16 v30, 0x76

    .line 84476434
    .line 84476435
    aput-byte v30, v4, v25

    .line 84476436
    .line 84476437
    const/16 v25, 0x20

    .line 84476438
    .line 84476439
    const/16 v30, 0x13

    .line 84476440
    .line 84476441
    aput-byte v30, v4, v25

    .line 84476442
    .line 84476443
    const/16 v25, 0x21

    .line 84476444
    .line 84476445
    aput-byte v16, v4, v25

    .line 84476446
    .line 84476447
    const/16 v25, 0x22

    .line 84476448
    .line 84476449
    aput-byte v15, v4, v25

    .line 84476450
    .line 84476451
    aput-byte v26, v4, v27

    .line 84476452
    .line 84476453
    const/16 v25, 0x24

    .line 84476454
    .line 84476455
    const/16 v30, 0x7b

    .line 84476456
    .line 84476457
    aput-byte v30, v4, v25

    .line 84476458
    .line 84476459
    const/16 v25, 0x43

    .line 84476460
    .line 84476461
    aput-byte v25, v4, v29

    .line 84476462
    .line 84476463
    const/16 v25, 0x26

    .line 84476464
    .line 84476465
    aput-byte v13, v4, v25

    .line 84476466
    .line 84476467
    const/16 v25, 0x24

    .line 84476468
    .line 84476469
    aput-byte v25, v4, v13

    .line 84476470
    .line 84476471
    const/16 v25, 0x2f

    .line 84476472
    .line 84476473
    aput-byte v25, v4, v11

    .line 84476474
    .line 84476475
    const/16 v25, 0x29

    .line 84476476
    .line 84476477
    const/16 v30, 0x6d

    .line 84476478
    .line 84476479
    aput-byte v30, v4, v25

    .line 84476480
    .line 84476481
    const/16 v25, 0x2a

    .line 84476482
    .line 84476483
    const/16 v30, 0x13

    .line 84476484
    .line 84476485
    aput-byte v30, v4, v25

    .line 84476486
    .line 84476487
    const/16 v25, 0x2b

    .line 84476488
    .line 84476489
    const/16 v30, 0x1a

    .line 84476490
    .line 84476491
    aput-byte v30, v4, v25

    .line 84476492
    .line 84476493
    const/16 v25, 0x2c

    .line 84476494
    .line 84476495
    const/16 v30, 0x36

    .line 84476496
    .line 84476497
    aput-byte v30, v4, v25

    .line 84476498
    .line 84476499
    const/16 v25, 0x2d

    .line 84476500
    .line 84476501
    const/16 v30, 0x2e

    .line 84476502
    .line 84476503
    aput-byte v30, v4, v25

    .line 84476504
    .line 84476505
    const/16 v25, 0x2e

    .line 84476506
    .line 84476507
    aput-byte v7, v4, v25

    .line 84476508
    .line 84476509
    const/16 v25, 0x2f

    .line 84476510
    .line 84476511
    const/16 v30, 0x58

    .line 84476512
    .line 84476513
    aput-byte v30, v4, v25

    .line 84476514
    .line 84476515
    aput-byte v6, v4, v28

    .line 84476516
    .line 84476517
    const/16 v25, 0x31

    .line 84476518
    .line 84476519
    const/16 v30, 0x37

    .line 84476520
    .line 84476521
    aput-byte v30, v4, v25

    .line 84476522
    .line 84476523
    const/16 v25, 0x34

    .line 84476524
    .line 84476525
    aput-byte v25, v4, v26

    .line 84476526
    .line 84476527
    const/16 v25, 0x33

    .line 84476528
    .line 84476529
    const/16 v30, 0x66

    .line 84476530
    .line 84476531
    aput-byte v30, v4, v25

    .line 84476532
    .line 84476533
    const/16 v25, 0x34

    .line 84476534
    .line 84476535
    aput-byte v8, v4, v25

    .line 84476536
    .line 84476537
    move-object/from16 v25, v4

    .line 84476538
    .line 84476539
    invoke-static/range {v20 .. v25}, Lms/bd/c/y2;->a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;

    .line 84476540
    .line 84476541
    .line 84476542
    move-result-object v4

    .line 84476543
    check-cast v4, Ljava/lang/String;

    .line 84476544
    .line 84476545
    invoke-static {v4}, Lms/bd/c/w;->a(Ljava/lang/String;)Ljava/lang/String;

    .line 84476546
    .line 84476547
    .line 84476548
    move-result-object v4

    .line 84476549
    invoke-virtual {v0, v3, v4}, Lorg/json/JSONObject;->put(Ljava/lang/String;Ljava/lang/Object;)Lorg/json/JSONObject;

    .line 84476550
    .line 84476551
    .line 84476552
    sget-object v3, Lms/bd/c/w;->h:Ljava/lang/String;

    .line 84476553
    .line 84476554
    const v20, 0x1000001

    .line 84476555
    .line 84476556
    .line 84476557
    const/16 v21, 0x0

    .line 84476558
    .line 84476559
    const-wide/16 v22, 0x0

    .line 84476560
    .line 84476561
    const-string v24, "1f9a9e"

    .line 84476562
    .line 84476563
    new-array v4, v7, [B

    .line 84476564
    .line 84476565
    const/16 v7, 0x6f

    .line 84476566
    .line 84476567
    aput-byte v7, v4, v12

    .line 84476568
    .line 84476569
    const/4 v7, 0x1

    .line 84476570
    const/16 v12, 0x77

    .line 84476571
    .line 84476572
    aput-byte v12, v4, v7

    .line 84476573
    .line 84476574
    const/16 v7, 0x53

    .line 84476575
    .line 84476576
    aput-byte v7, v4, v1

    .line 84476577
    .line 84476578
    aput-byte v19, v4, v10

    .line 84476579
    .line 84476580
    const/16 v7, 0x49

    .line 84476581
    .line 84476582
    aput-byte v7, v4, v8

    .line 84476583
    .line 84476584
    const/16 v7, 0x76

    .line 84476585
    .line 84476586
    aput-byte v7, v4, v17

    .line 84476587
    .line 84476588
    const/16 v7, 0x37

    .line 84476589
    .line 84476590
    aput-byte v7, v4, v19

    .line 84476591
    .line 84476592
    const/16 v7, 0x51

    .line 84476593
    .line 84476594
    aput-byte v7, v4, v16

    .line 84476595
    .line 84476596
    const/16 v7, 0x61

    .line 84476597
    .line 84476598
    aput-byte v7, v4, v6

    .line 84476599
    .line 84476600
    const/16 v6, 0x9

    .line 84476601
    .line 84476602
    aput-byte v26, v4, v6

    .line 84476603
    .line 84476604
    aput-byte v29, v4, v5

    .line 84476605
    .line 84476606
    const/16 v5, 0xb

    .line 84476607
    .line 84476608
    const/16 v6, 0x77

    .line 84476609
    .line 84476610
    aput-byte v6, v4, v5

    .line 84476611
    .line 84476612
    aput-byte v17, v4, v15

    .line 84476613
    .line 84476614
    const/16 v5, 0xd

    .line 84476615
    .line 84476616
    aput-byte v19, v4, v5

    .line 84476617
    .line 84476618
    const/16 v5, 0x1f

    .line 84476619
    .line 84476620
    aput-byte v5, v4, v18

    .line 84476621
    .line 84476622
    const/16 v5, 0xf

    .line 84476623
    .line 84476624
    const/16 v6, 0x61

    .line 84476625
    .line 84476626
    aput-byte v6, v4, v5

    .line 84476627
    .line 84476628
    const/16 v5, 0x26

    .line 84476629
    .line 84476630
    aput-byte v5, v4, v14

    .line 84476631
    .line 84476632
    const/16 v5, 0x11

    .line 84476633
    .line 84476634
    const/16 v6, 0x42

    .line 84476635
    .line 84476636
    aput-byte v6, v4, v5

    .line 84476637
    .line 84476638
    const/16 v5, 0x12

    .line 84476639
    .line 84476640
    const/16 v6, 0x65

    .line 84476641
    .line 84476642
    aput-byte v6, v4, v5

    .line 84476643
    .line 84476644
    const/16 v5, 0x7e

    .line 84476645
    .line 84476646
    const/16 v6, 0x13

    .line 84476647
    .line 84476648
    aput-byte v5, v4, v6

    .line 84476649
    .line 84476650
    const/16 v5, 0x14

    .line 84476651
    .line 84476652
    aput-byte v27, v4, v5

    .line 84476653
    .line 84476654
    const/16 v5, 0x15

    .line 84476655
    .line 84476656
    const/16 v6, 0x74

    .line 84476657
    .line 84476658
    aput-byte v6, v4, v5

    .line 84476659
    .line 84476660
    const/16 v5, 0x5f

    .line 84476661
    .line 84476662
    aput-byte v5, v4, v9

    .line 84476663
    .line 84476664
    const/16 v5, 0x5a

    .line 84476665
    .line 84476666
    const/16 v6, 0x17

    .line 84476667
    .line 84476668
    aput-byte v5, v4, v6

    .line 84476669
    .line 84476670
    const/16 v5, 0x18

    .line 84476671
    .line 84476672
    aput-byte v17, v4, v5

    .line 84476673
    .line 84476674
    const/16 v5, 0x19

    .line 84476675
    .line 84476676
    const/16 v6, 0x62

    .line 84476677
    .line 84476678
    aput-byte v6, v4, v5

    .line 84476679
    .line 84476680
    const/16 v5, 0x1a

    .line 84476681
    .line 84476682
    aput-byte v13, v4, v5

    .line 84476683
    .line 84476684
    const/16 v5, 0x1b

    .line 84476685
    .line 84476686
    const/16 v6, 0x17

    .line 84476687
    .line 84476688
    aput-byte v6, v4, v5

    .line 84476689
    .line 84476690
    const/16 v5, 0x1c

    .line 84476691
    .line 84476692
    aput-byte v13, v4, v5

    .line 84476693
    .line 84476694
    const/16 v5, 0x1d

    .line 84476695
    .line 84476696
    aput-byte v26, v4, v5

    .line 84476697
    .line 84476698
    const/16 v5, 0x1e

    .line 84476699
    .line 84476700
    aput-byte v28, v4, v5

    .line 84476701
    .line 84476702
    const/16 v5, 0x1f

    .line 84476703
    .line 84476704
    const/16 v6, 0x71

    .line 84476705
    .line 84476706
    aput-byte v6, v4, v5

    .line 84476707
    .line 84476708
    const/16 v5, 0x20

    .line 84476709
    .line 84476710
    const/16 v6, 0x4c

    .line 84476711
    .line 84476712
    aput-byte v6, v4, v5

    .line 84476713
    .line 84476714
    const/16 v5, 0x21

    .line 84476715
    .line 84476716
    aput-byte v16, v4, v5

    .line 84476717
    .line 84476718
    const/16 v5, 0x22

    .line 84476719
    .line 84476720
    aput-byte v10, v4, v5

    .line 84476721
    .line 84476722
    const/16 v5, 0x63

    .line 84476723
    .line 84476724
    aput-byte v5, v4, v27

    .line 84476725
    .line 84476726
    const/16 v5, 0x24

    .line 84476727
    .line 84476728
    const/16 v6, 0x7d

    .line 84476729
    .line 84476730
    aput-byte v6, v4, v5

    .line 84476731
    .line 84476732
    const/16 v5, 0x44

    .line 84476733
    .line 84476734
    aput-byte v5, v4, v29

    .line 84476735
    .line 84476736
    const/16 v5, 0x26

    .line 84476737
    .line 84476738
    const/16 v6, 0x78

    .line 84476739
    .line 84476740
    aput-byte v6, v4, v5

    .line 84476741
    .line 84476742
    const/16 v5, 0x24

    .line 84476743
    .line 84476744
    aput-byte v5, v4, v13

    .line 84476745
    .line 84476746
    const/16 v5, 0x29

    .line 84476747
    .line 84476748
    aput-byte v5, v4, v11

    .line 84476749
    .line 84476750
    const/16 v5, 0x29

    .line 84476751
    .line 84476752
    const/16 v6, 0x6a

    .line 84476753
    .line 84476754
    aput-byte v6, v4, v5

    .line 84476755
    .line 84476756
    const/16 v5, 0x2a

    .line 84476757
    .line 84476758
    const/16 v6, 0x4c

    .line 84476759
    .line 84476760
    aput-byte v6, v4, v5

    .line 84476761
    .line 84476762
    const/16 v5, 0x2b

    .line 84476763
    .line 84476764
    const/16 v6, 0x1a

    .line 84476765
    .line 84476766
    aput-byte v6, v4, v5

    .line 84476767
    .line 84476768
    const/16 v5, 0x2c

    .line 84476769
    .line 84476770
    const/16 v6, 0x39

    .line 84476771
    .line 84476772
    aput-byte v6, v4, v5

    .line 84476773
    .line 84476774
    const/16 v5, 0x2d

    .line 84476775
    .line 84476776
    const/16 v6, 0x7f

    .line 84476777
    .line 84476778
    aput-byte v6, v4, v5

    .line 84476779
    .line 84476780
    const/16 v5, 0x2e

    .line 84476781
    .line 84476782
    const/16 v6, 0x3b

    .line 84476783
    .line 84476784
    aput-byte v6, v4, v5

    .line 84476785
    .line 84476786
    const/16 v5, 0x2f

    .line 84476787
    .line 84476788
    const/16 v6, 0x49

    .line 84476789
    .line 84476790
    aput-byte v6, v4, v5

    .line 84476791
    .line 84476792
    const/16 v5, 0x57

    .line 84476793
    .line 84476794
    aput-byte v5, v4, v28

    .line 84476795
    .line 84476796
    const/16 v5, 0x31

    .line 84476797
    .line 84476798
    const/16 v6, 0x37

    .line 84476799
    .line 84476800
    aput-byte v6, v4, v5

    .line 84476801
    .line 84476802
    aput-byte v26, v4, v26

    .line 84476803
    .line 84476804
    const/16 v5, 0x33

    .line 84476805
    .line 84476806
    const/16 v6, 0x61

    .line 84476807
    .line 84476808
    aput-byte v6, v4, v5

    .line 84476809
    .line 84476810
    const/16 v5, 0x34

    .line 84476811
    .line 84476812
    const/16 v6, 0x5b

    .line 84476813
    .line 84476814
    aput-byte v6, v4, v5

    .line 84476815
    .line 84476816
    move-object/from16 v25, v4

    .line 84476817
    .line 84476818
    invoke-static/range {v20 .. v25}, Lms/bd/c/y2;->a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;

    .line 84476819
    .line 84476820
    .line 84476821
    move-result-object v4

    .line 84476822
    check-cast v4, Ljava/lang/String;

    .line 84476823
    .line 84476824
    invoke-static {v4}, Lms/bd/c/w;->a(Ljava/lang/String;)Ljava/lang/String;

    .line 84476825
    .line 84476826
    .line 84476827
    move-result-object v4

    .line 84476828
    invoke-virtual {v0, v3, v4}, Lorg/json/JSONObject;->put(Ljava/lang/String;Ljava/lang/Object;)Lorg/json/JSONObject;

    .line 84476829
    .line 84476830
    .line 84476831
    sget-object v3, Lms/bd/c/w;->c:Ljava/lang/String;

    .line 84476832
    .line 84476833
    invoke-static {v2, v3}, Lms/bd/c/w;->b(Ljava/util/HashMap;Ljava/lang/String;)Ljava/lang/String;

    .line 84476834
    .line 84476835
    .line 84476836
    move-result-object v2

    .line 84476837
    sget-object v3, Lms/bd/c/w;->f:Ljava/lang/String;

    .line 84476838
    .line 84476839
    invoke-virtual {v0, v3, v2}, Lorg/json/JSONObject;->put(Ljava/lang/String;Ljava/lang/Object;)Lorg/json/JSONObject;
    :try_end_4
    .catchall {:try_start_4 .. :try_end_4} :catchall_3

    .line 84476840
    .line 84476841
    .line 84476842
    goto :goto_3

    .line 84476843
    :catchall_3
    nop

    .line 84476844
    :goto_3
    invoke-virtual {v0}, Lorg/json/JSONObject;->toString()Ljava/lang/String;

    .line 84476845
    .line 84476846
    .line 84476847
    move-result-object v0

    .line 84476848
    invoke-static {v0}, Landroid/text/TextUtils;->isEmpty(Ljava/lang/CharSequence;)Z

    .line 84476849
    .line 84476850
    .line 84476851
    move-result v2

    .line 84476852
    if-eqz v2, :cond_3

    .line 84476853
    .line 84476854
    new-array v0, v1, [B

    .line 84476855
    .line 84476856
    fill-array-data v0, :array_0

    .line 84476857
    .line 84476858
    .line 84476859
    const v1, 0x1000001

    .line 84476860
    .line 84476861
    .line 84476862
    const/4 v2, 0x0

    .line 84476863
    const-wide/16 v3, 0x0

    .line 84476864
    .line 84476865
    const-string v5, "a54535"

    .line 84476866
    .line 84476867
    move/from16 p0, v1

    .line 84476868
    .line 84476869
    move/from16 p1, v2

    .line 84476870
    .line 84476871
    move-wide/from16 p2, v3

    .line 84476872
    .line 84476873
    move-object/from16 p4, v5

    .line 84476874
    .line 84476875
    move-object/from16 p5, v0

    .line 84476876
    .line 84476877
    invoke-static/range {p0 .. p5}, Lms/bd/c/y2;->a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;

    .line 84476878
    .line 84476879
    .line 84476880
    move-result-object v0

    .line 84476881
    check-cast v0, Ljava/lang/String;

    .line 84476882
    .line 84476883
    goto :goto_4

    .line 84476884
    :cond_3
    invoke-virtual {v0}, Ljava/lang/String;->trim()Ljava/lang/String;

    .line 84476885
    .line 84476886
    .line 84476887
    move-result-object v0

    .line 84476888
    goto :goto_4

    .line 84476889
    :pswitch_d
    sget v0, Lms/bd/c/n4;->a:I

    .line 84476890
    .line 84476891
    const-string v0, ""

    .line 84476892
    .line 84476893
    goto :goto_4

    .line 84476894
    :pswitch_e
    const-string v0, ""

    .line 84476895
    .line 84476896
    invoke-static {v0}, Lms/bd/c/n4;->a(Ljava/lang/String;)Ljava/lang/String;

    .line 84476897
    .line 84476898
    .line 84476899
    move-result-object v0

    .line 84476900
    goto :goto_4

    .line 84476901
    :pswitch_f
    const-string v0, "np"

    .line 84476902
    .line 84476903
    goto :goto_4

    .line 84476904
    :pswitch_10
    sget-object v0, Lms/bd/c/m2;->b:Lms/bd/c/m2;

    .line 84476905
    .line 84476906
    iget-object v0, v0, Lms/bd/c/m2;->a:Landroid/content/Context;

    .line 84476907
    .line 84476908
    invoke-static {v0}, Lms/bd/c/r3;->a(Landroid/content/Context;)Ljava/lang/String;

    .line 84476909
    .line 84476910
    .line 84476911
    move-result-object v0

    .line 84476912
    :goto_4
    return-object v0

    .line 84476913
    :cond_4
    sget-object v1, Lms/bd/c/x2;->a:Lms/bd/c/q4;

    .line 84476914
    .line 84476915
    const/high16 v1, 0x10000

    .line 84476916
    .line 84476917
    if-le v0, v1, :cond_5

    .line 84476918
    .line 84476919
    const v1, 0x10009

    .line 84476920
    .line 84476921
    .line 84476922
    if-ge v0, v1, :cond_5

    .line 84476923
    .line 84476924
    goto :goto_5

    .line 84476925
    :cond_5
    const/high16 v1, 0x30000

    .line 84476926
    .line 84476927
    if-le v0, v1, :cond_6

    .line 84476928
    .line 84476929
    const v1, 0x30006

    .line 84476930
    .line 84476931
    .line 84476932
    if-ge v0, v1, :cond_6

    .line 84476933
    .line 84476934
    goto :goto_5

    .line 84476935
    :cond_6
    const/high16 v1, 0x20000

    .line 84476936
    .line 84476937
    if-le v0, v1, :cond_7

    .line 84476938
    .line 84476939
    const v1, 0x20003

    .line 84476940
    .line 84476941
    .line 84476942
    if-ge v0, v1, :cond_7

    .line 84476943
    .line 84476944
    :goto_5
    invoke-static/range {p0 .. p5}, Lms/bd/c/x2;->d(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/io/Serializable;

    .line 84476945
    .line 84476946
    .line 84476947
    move-result-object v0

    .line 84476948
    return-object v0

    .line 84476949
    :cond_7
    sget-object v1, Lms/bd/c/s2;->a:Lms/bd/c/s4;

    .line 84476950
    .line 84476951
    const/high16 v1, 0x1000000

    .line 84476952
    .line 84476953
    if-le v0, v1, :cond_8

    .line 84476954
    .line 84476955
    const v1, 0x1000044

    .line 84476956
    .line 84476957
    .line 84476958
    if-ge v0, v1, :cond_8

    .line 84476959
    .line 84476960
    goto :goto_6

    .line 84476961
    :cond_8
    const/high16 v1, 0x2000000

    .line 84476962
    .line 84476963
    if-le v0, v1, :cond_9

    .line 84476964
    .line 84476965
    const v1, 0x2000003

    .line 84476966
    .line 84476967
    .line 84476968
    if-ge v0, v1, :cond_9

    .line 84476969
    .line 84476970
    :goto_6
    invoke-static/range {p0 .. p5}, Lms/bd/c/s2;->h(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;

    .line 84476971
    .line 84476972
    .line 84476973
    move-result-object v0

    .line 84476974
    return-object v0

    .line 84476975
    :cond_9
    const/4 v0, 0x0

    .line 84476976
    return-object v0

    .line 84476977
    nop

    .line 84476978
    :pswitch_data_0
    .packed-switch 0x10000001
        :pswitch_10
        :pswitch_f
        :pswitch_f
        :pswitch_e
        :pswitch_d
        :pswitch_c
        :pswitch_b
        :pswitch_a
        :pswitch_9
        :pswitch_0
        :pswitch_8
        :pswitch_7
        :pswitch_6
        :pswitch_5
        :pswitch_4
        :pswitch_3
        :pswitch_2
        :pswitch_1
    .end packed-switch

    .line 84476979
    .line 84476980
    .line 84476981
    .line 84476982
    .line 84476983
    .line 84476984
    .line 84476985
    .line 84476986
    .line 84476987
    .line 84476988
    .line 84476989
    .line 84476990
    .line 84476991
    .line 84476992
    .line 84476993
    .line 84476994
    .line 84476995
    .line 84476996
    .line 84476997
    .line 84476998
    .line 84476999
    .line 84477000
    .line 84477001
    .line 84477002
    .line 84477003
    .line 84477004
    .line 84477005
    .line 84477006
    .line 84477007
    .line 84477008
    .line 84477009
    .line 84477010
    .line 84477011
    .line 84477012
    .line 84477013
    .line 84477014
    .line 84477015
    .line 84477016
    .line 84477017
    .line 84477018
    :array_0
    .array-data 1
        0x6bt
        0x2at
    .end array-data
.end method
