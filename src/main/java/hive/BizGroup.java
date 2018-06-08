package hive;

/**
 * @program: phoenixtest
 * @description:
 * @author: jiangyun
 * @create: 2018-05-30 15:36
 **/
public class BizGroup {
    private int bizGroupId;
    private int bizGroupType;
    private int relationType;


    public int getBizGroupId() {
        return bizGroupId;
    }

    public void setBizGroupId(int bizGroupId) {
        this.bizGroupId = bizGroupId;
    }

    public int getBizGroupType() {
        return bizGroupType;
    }

    public void setBizGroupType(int bizGroupType) {
        this.bizGroupType = bizGroupType;
    }

    public int getRelationType() {
        return relationType;
    }

    public void setRelationType(int relationType) {
        this.relationType = relationType;
    }

    @Override
    public String toString() {
        return "BizGroup{" +
                "bizGroupId=" + bizGroupId +
                ", bizGroupType=" + bizGroupType +
                ", relationType=" + relationType +
                '}';
    }
}