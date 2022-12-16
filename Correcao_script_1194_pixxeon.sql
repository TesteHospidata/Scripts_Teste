DROP VIEW integracao.pixeon_worklist_v;

CREATE OR REPLACE VIEW integracao.pixeon_worklist_v
AS
select
    t.na_accessionnumber,
    t.co_patientid,
    t.na_patientname,
    case when t.na_patientsex = 'MASCULINO' then 'M' when t.na_patientsex = 'FEMININO' then 'F' else 'O' end na_patientsex,
    t.na_patientbirthday,
    t.na_patientweight,
    t.na_patientheight,
    t.co_performingid,
    t.na_performingname,
    t.na_performingcrm,
    t.na_priority,
    t.na_performinguf,
    t.na_performingemail,
    t.co_requesterid,
    t.na_requestername,
    t.na_requestercrm,
    t.na_requesteremail,
    t.na_requesteruf,
    proc.descr_proc as na_description,
    t.na_bodypart,
    t.na_studydate,
    t.na_studytime,
    e.nm_equipamento as na_modalityris,
    t.na_requestercrm as na_password,
    t.na_requireunit,
    t.na_datetimerelease,
    e.id_equipamento as na_studyid,
    t.na_machine,
    t.na_insuranceplan,
    t.na_requestingdepartment ,
    t.na_requesterphone ,
    t.na_patientrg ,
    t.na_patientphone ,
    t.na_patientemail ,
    t.na_patientcpf ,
    t.na_idexame ,
    t.na_insurancecarrier ,
    t.na_idinsurancecarrier ,
    t.na_idinsuranceplan ,
    t.na_procedencia ,
    t.na_requisicao
from
    (
    select
        l.numero_exame as numero_laudo,
        to_char(lanc.data::timestamp with time zone, 'yyyymmdd'::text) as na_studydate,
        to_char(lanc.hora::interval, 'hh24miss'::text) as na_studytime,
        pres.codigo_reduzido as co_performingid,
        pi.nm_prestador as na_performingname,
        preq.crm_prestador as na_requestercrm,
        fia.urgente_eletivo as na_priority,
        p.nm_paciente as na_patientname,
        p.registro as co_patientid,
        p.peso_paciente as na_patientweight,
        p.altura_paciente as na_patientheight,
        s.nm_sexo as na_patientsex,
        pi.crm_prestador as na_performingcrm,
        ufs_pi.sigla as na_performinguf,
        pi.e_mail_prestador as na_performingemail,    
        preq.e_mail_prestador as na_requesteremail,
        pres.codigo_reduzido as co_requesterid,
        preq.nm_prestador as na_requestername,
        ufs_preq.sigla as na_requesteruf,
        to_char(p.data_nasc::timestamp with time zone, 'yyyymmdd'::text) as na_patientbirthday,
        min(lanc.id_lancamento) as na_accessionnumber,
        l.cod_equipamento as na_studyid,
        (
        select
            null::character varying as "varchar") as na_bodypart,
        fia.cod_unidade as na_requireunit,
        to_char(l.previsao_entrega::timestamp with time zone, 'yyyymmdd'::text) as na_datetimerelease,
        (
        select
            null::character varying as "varchar") as na_machine,
        uni.nm_unidade na_requestingservice,
        con.nm_convenio as na_insuranceplan,
        spo.nm_setor_posto as na_requestingdepartment,
        coalesce(preq.fone_cel_prestador,preq.fone_prestador,preq.fone_resid_prestador,preq.fone_cel_opcional_prestador) as na_requesterphone,
        (select dpr.numero_documento  from sigh.documentos_pacientes dpr where dpr.cod_paciente = p.id_paciente and dpr.cod_tp_documento = 2 limit 1) as na_patientrg,
        coalesce(p.fone_cel_1,p.fone_cel_2,p.fone_res_1,p.fone_res_2) as na_patientphone,
        p.email as na_patientemail,
        (select dpc.numero_documento  from sigh.documentos_pacientes dpc where dpc.cod_paciente = p.id_paciente and dpc.cod_tp_documento = 4 limit 1) as na_patientcpf,
        pro.codigo_procedimento  as na_idexame,
        con.id_convenio as na_idinsurancecarrier,
        con.nm_convenio as na_insurancecarrier,
        fia.cod_categoria  as na_idinsuranceplan,
        case  
            when fia.tipo_atend = 'INT' then 'INTERNAÇÃO'
            when fia.tipo_atend = 'EXT' then 'EXTERNO'
            when fia.tipo_atend = 'AMB' then 'AMBULATORIAL'
            else fia.tipo_atend
        end as na_procedencia,
        coalesce (l.cod_prescricao,l.numero_exame ) as na_requisicao
    from
        sigh.laudos l
    join sigh.ficha_amb_int fia on
        fia.id_fia = l.cod_fia
    left join sigh.pacientes p on
        p.id_paciente = fia.cod_paciente
    left join sigh.sexos s on
        s.id_sexo = p.cod_sexo
    left join sigh.prestadores pi on
        pi.id_prestador = l.cod_medico_interp
    left join sigh.prestadores pres on
        pres.id_prestador = l.cod_medico_resp
    left join sigh.prestadores preq on
        preq.id_prestador = l.cod_medico_req
    join sigh.lancamentos lanc on
        lanc.cod_laudo = l.id_laudo
    left join sigh.prestadores pree on
        lanc.cod_prestador = pree.id_prestador
    left join endereco_sigh.ufs ufs_pi on
        ufs_pi.id_uf = pi.cod_uf
    left join endereco_sigh.ufs ufs_preq on
        ufs_preq.id_uf = preq.cod_uf
    join sigh.convenios con on
        con.id_convenio = fia.cod_convenio
    left join sigh.leitos lei ON
        lei.id_leito = fia.cod_leito
    left join sigh.quartos_enfermarias que ON
        que.id_quarto_enf = lei.cod_quarto_enf
    left join sigh.tipos_leitos tlei ON
        tlei.id_tp_leito = que.cod_tp_leito
    left join sigh.setores_postos spo ON
        spo.id_setor_posto = que.cod_setor_posto
    left join sigh.unidades uni ON
        uni.id_unidade = spo.cod_unidade
    left join sigh.procedimentos pro ON
        pro.id_procedimento = lanc.cod_proc
    where
        l.cod_hospital = ((
        select
            util.f_hospital_corrente() as f_hospital_corrente))
        and fia.data_alta is null
        and l.ativo
        and lanc.ativo
        and lanc.integrado_pixeon = false
    group by
        p.id_paciente,
        pres.codigo_reduzido,
        preq.e_mail_prestador,
        pi.e_mail_prestador,
        p.altura_paciente,
        l.id_laudo,
        l.numero_exame,
        l.data_realizado_exame,
        l.hora_realizado,
        p.nm_paciente,
        s.nm_sexo,
        p.data_nasc,
        p.registro,
        p.peso_paciente,
        pi.nm_prestador,
        pi.crm_prestador,
        l.laudo,
        l.cod_equipamento,
        l.laudo_texto,
        l.data_exame,
        pres.nm_prestador,
        preq.nm_prestador,
        preq.crm_prestador,
        fia.tipo_atend,
        fia.id_fia,
        pree.nm_prestador,
        lanc.id_lancamento,
        ufs_pi.sigla,
        ufs_preq.sigla,
        con.nm_convenio,
        uni.nm_unidade,
        spo.nm_setor_posto,
        preq.fone_cel_prestador,
        preq.fone_prestador,
        preq.fone_resid_prestador,
        preq.fone_cel_opcional_prestador,
        p.fone_cel_1,
        p.fone_cel_2,
        p.fone_res_1,
        p.fone_res_2,
        p.email,
        fia.urgente_eletivo,
        pro.codigo_procedimento,
        con.id_convenio,
        con.nm_convenio,
        l.cod_prescricao) t
left join sigh.lancamentos lanc on
    lanc.id_lancamento = t.na_accessionnumber
left join sigh.procedimentos proc on
    proc.id_procedimento = lanc.cod_proc
left join sigh.equipamentos_rx e on
    e.id_equipamento = t.na_studyid;
 
ALTER TABLE integracao.pixeon_worklist_v 
OWNER TO hd;
GRANT ALL ON TABLE integracao.pixeon_worklist_v TO hd;
GRANT ALL ON TABLE integracao.pixeon_worklist_v TO admin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE integracao.pixeon_worklist_v TO consultas;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE integracao.pixeon_worklist_v TO hd_integracao;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE integracao.pixeon_worklist_v TO hd_integra_pac;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE integracao.pixeon_worklist_v TO hd_suporte;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE integracao.pixeon_worklist_v TO hd_faturamento;
 

--- ATUALIZAR TABELA---
alter table integracao.pixeon_integracao add column if not exists status varchar(16) null;
alter table integracao.pixeon_integracao add column if not exists  message varchar (2048) NULL;
