$(function(){
    svg_wrapper = $("#svgArea")
    svg_wrapper.load("/static/images/face_normal.svg svg");

    $("#to-negative").on("click", function(){
        svg_wrapper.load("/static/images/face_negative.svg svg");
    });

    $("#to-normal").on("click", function(){
        svg_wrapper.load("/static/images/face_normal.svg svg");
    });

    $("#to-positive").on("click", function(){
        svg_wrapper.load("/static/images/face_positive.svg svg");
    });

    $("#to-think").on("click", function(){
        svg_wrapper.load("/static/images/face_think.svg svg");
    });

    $("#submit").on("click", function(){
        var keywords = $("#keywords").val();
        svg_wrapper.load("/static/images/face_think.svg svg");
        $.ajax({
            type: "POST",
            url:  "/classify",
            data: {"keywords": keywords},
            success: function(data){
                svg_wrapper.load("/static/images/face_positive.svg svg");                
                $("#result").text(data);
                setTimeout(function(){
                    svg_wrapper.load("/static/images/face_normal.svg svg");
                }, 5000);
            }
        });
    });

});
